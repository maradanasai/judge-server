import logging
from base64 import b64decode
from enum import Enum
from operator import itemgetter
from typing import cast

import connexion
import waitress
import signal
import traceback
from flask import g, jsonify, Flask, request
from flask_cors import CORS

from dmoj import judgeenv, executors
from dmoj.judge import Judge, Submission
from dmoj.monitor import Monitor
from dmoj.packet import PacketManager
from dmoj.utils.ansi import ansi_style
from dmoj.utils.unicode import utf8text

app = Flask(__name__)

class JudgeState(Enum):
    FAILED = 0
    SUCCESS = 1


class LocalPacketManager(object):
    def __init__(self, judge):
        self.judge = judge

    def _receive_packet(self, packet):
        pass

    def supported_problems_packet(self, problems):
        pass

    def test_case_status_packet(self, position, result):
        self.judge.graded_submissions[-1]['testCaseResults'].append({
                'case': position,
                'verdict': result.readable_codes()[0],
                'input': utf8text(result.input_case),
                'output': utf8text(result.output_case),
                'yourOutput': result.output
            })

    def compile_error_packet(self, message):
        self.judge.graded_submissions[-1]['compileError'].append(message)

    def compile_message_packet(self, message):
        pass

    def internal_error_packet(self, message):
        self.judge.graded_submissions[-1]['internalError'].append(message)

    def begin_grading_packet(self, is_pretested):
        pass

    def grading_end_packet(self):
        pass

    def batch_begin_packet(self):
        pass

    def batch_end_packet(self):
        pass

    def current_submission_packet(self):
        pass

    def submission_terminated_packet(self):
        pass

    def submission_acknowledged_packet(self, sub_id):
        pass

    def run(self):
        pass

    def close(self):
        pass


class LocalJudge(Judge):
    # graded_submissions: List[GradedSubmission]
    def __init__(self):
        super().__init__(cast(PacketManager, LocalPacketManager(self)))
        self.next_submission_id = 0
        self.graded_submissions = []


def get_judge():
    judge = getattr(g, '_judge', None)
    if judge is None:
        g._judge = LocalJudge()
    return g._judge

def get_all_problems():
    return jsonify(list(judgeenv.get_supported_problems())), 200

# POST /submission
@app.route("/submit", methods = ['POST'])
def add_submission():
    judge = get_judge()
    body = request.get_json(force=True)
    problem_id = body['problemId']
    language_id = body['languageId']
    time_limit = body['timeLimit']
    memory_limit = body['memoryLimit']
    source = body['sourceCode']

    if problem_id not in map(itemgetter(0), judgeenv.get_supported_problems_and_mtimes()):
        return jsonify({
            'error': "unknown problem %s" % problem_id
        }), 405

    if language_id not in executors.executors:
        return jsonify({'error': "unknown language %s" % language_id}), 405

    if time_limit <= 0:
        return jsonify({'error': "timeLimit must be >= 0"}), 405

    if memory_limit <= 0:
        return jsonify({'error': "memoryLimit must be >= 0"}), 405

    submission_id = judge.next_submission_id

    judge.graded_submissions.append({
        "problemId": problem_id,
        "languageId": language_id,
        "compileError": [],
        "testCaseResults": [],
        "internalError": []
    })

    source = b64decode(source).decode('utf-8')

    judge.begin_grading(
        Submission(
            judge.next_submission_id, problem_id, language_id, source, time_limit, memory_limit, False, {}
        ),
        blocking=True,
        report=print
    )

    judge.next_submission_id += 1

    return jsonify(judge.graded_submissions[submission_id]), 200


# GET /runtimes
@app.route("/runtimes")
def get_all_runtimes():
    return jsonify(list(executors.executors.keys())), 200


def main():
    judgeenv.load_env(cli=False)
    executors.load_executors()

    logging.basicConfig(filename=judgeenv.log_file, level=logging.INFO,
                        format='%(levelname)s %(asctime)s %(module)s %(message)s')

    for warning in judgeenv.startup_warnings:
        print(ansi_style('#ansi[Warning: %s](yellow)' % warning))
    del judgeenv.startup_warnings
    print()

    CORS(app)
    with app.app_context():
        judge = get_judge()
        monitor = Monitor()
        monitor.callback = judge.update_problems

        if hasattr(signal, 'SIGUSR2'):
            def update_problem_signal(signum, frame):
                judge.update_problems()
            signal.signal(signal.SIGUSR2, update_problem_signal)
        with monitor:
            try:
                judge.listen()
            except KeyboardInterrupt:
                pass
            except Exception:
                traceback.print_exc()
            finally:
                judge.murder()
        #app.run(port=8080, debug=False)
        from waitress import serve
        serve(app, port=8080)


if __name__ == '__main__':
    main()
