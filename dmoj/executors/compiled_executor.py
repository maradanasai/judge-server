import hashlib
import os
import pty
import struct
import sys
from typing import Any, Dict, IO, List, Optional, Sequence

import pylru

from dmoj.cptbox import IsolateTracer, TracedPopen
from dmoj.cptbox._cptbox import AT_FDCWD, Debugger
from dmoj.cptbox.filesystem_policies import ExactFile, FilesystemAccessRule, RecursiveDir
from dmoj.cptbox.handlers import ACCESS_EFAULT, ACCESS_EPERM, ALLOW
from dmoj.cptbox.syscalls import *
from dmoj.cptbox.tracer import AdvancedDebugger
from dmoj.error import CompileError, OutputLimitExceeded
from dmoj.executors.base_executor import BASE_FILESYSTEM, BASE_WRITE_FILESYSTEM, BaseExecutor, ExecutorMeta
from dmoj.judgeenv import env
from dmoj.utils.communicate import safe_communicate
from dmoj.utils.error import print_protection_fault
from dmoj.utils.unicode import utf8bytes


# A lot of executors must do initialization during their constructors, which is
# complicated by the CompiledExecutor compiling *during* its constructor. From a
# user's perspective, though, once an Executor is instantiated, it should be ready
# to launch (e.g. the user shouldn't have to care about compiling themselves). As
# a compromise, we use a metaclass to compile after all constructors have ran.
#
# Using a metaclass also allows us to handle caching executors transparently.
# Contract: if cached=True is specified and an entry exists in the cache,
# `create_files` and `compile` will not be run, and `_executable` will be loaded
# from the cache.
class _CompiledExecutorMeta(ExecutorMeta):
    @staticmethod
    def _cleanup_cache_entry(_key, executor: 'CompiledExecutor') -> None:
        # Mark the executor as not-cached, so that if this is the very last reference
        # to it, __del__ will clean it up.
        executor.is_cached = False

    compiled_binary_cache: Dict[str, 'CompiledExecutor'] = pylru.lrucache(
        env.compiled_binary_cache_size, _cleanup_cache_entry
    )

    def __call__(cls, *args, **kwargs) -> 'CompiledExecutor':
        is_cached: bool = kwargs.pop('cached', False)
        if is_cached:
            kwargs['dest_dir'] = env.compiled_binary_cache_dir

        # Finish running all constructors before compiling.
        obj: 'CompiledExecutor' = super().__call__(*args, **kwargs)
        obj.is_cached = is_cached

        # Before writing sources to disk, check if we have this executor in our cache.
        if is_cached:
            cache_key_material = utf8bytes(obj.__class__.__name__ + obj.__module__) + obj.get_binary_cache_key()
            cache_key = hashlib.sha384(cache_key_material).hexdigest()
            if cache_key in cls.compiled_binary_cache:
                executor = cls.compiled_binary_cache[cache_key]
                assert executor._executable is not None
                # Minimal sanity checking: is the file still there? If not, we'll just recompile.
                if os.path.isfile(executor._executable):
                    obj._executable = executor._executable
                    obj._dir = executor._dir
                    return obj

        obj.create_files(*args, **kwargs)
        obj.compile()

        if is_cached:
            cls.compiled_binary_cache[cache_key] = obj

        return obj


UTIME_OMIT = (1 << 30) - 2


class CompilerIsolateTracer(IsolateTracer):
    def __init__(self, tmpdir, read_fs, write_fs, *args, **kwargs):
        read_fs += BASE_FILESYSTEM + [
            RecursiveDir(tmpdir),
            ExactFile('/bin/strip'),
            RecursiveDir('/usr/x86_64-linux-gnu'),
        ]
        write_fs += BASE_WRITE_FILESYSTEM + [RecursiveDir(tmpdir)]
        super().__init__(read_fs, *args, write_fs=write_fs, **kwargs)

        self.update(
            {
                # Process spawning system calls
                sys_fork: ALLOW,
                sys_vfork: ALLOW,
                sys_execve: ALLOW,
                sys_getcpu: ALLOW,
                sys_getpgid: ALLOW,
                # Directory system calls
                sys_mkdir: self.check_file_access('mkdir', 0, is_write=True),
                sys_mkdirat: self.check_file_access_at('mkdirat', is_write=True),
                sys_rmdir: self.check_file_access('rmdir', 0, is_write=True),
                # Linking system calls
                sys_link: self.check_file_access('link', 1, is_write=True),
                sys_linkat: self.check_file_access_at('linkat', argument=3, is_write=True),
                sys_unlink: self.check_file_access('unlink', 0, is_write=True),
                sys_unlinkat: self.check_file_access_at('unlinkat', is_write=True),
                sys_symlink: self.check_file_access('symlink', 1, is_write=True),
                # Miscellaneous other filesystem system calls
                sys_chdir: self.check_file_access('chdir', 0),
                sys_chmod: self.check_file_access('chmod', 0, is_write=True),
                sys_utimensat: self.do_utimensat,
                sys_statx: self.check_file_access_at('statx'),
                sys_umask: ALLOW,
                sys_flock: ALLOW,
                sys_fsync: ALLOW,
                sys_fadvise64: ALLOW,
                sys_fchmodat: self.check_file_access_at('fchmodat', is_write=True),
                sys_fchmod: self.do_fchmod,
                sys_fallocate: ALLOW,
                sys_ftruncate: ALLOW,
                sys_rename: self.do_rename,
                sys_renameat: self.do_renameat,
                # I/O system calls
                sys_readv: ALLOW,
                sys_pwrite64: ALLOW,
                sys_sendfile: ALLOW,
                # Event loop system calls
                sys_epoll_create: ALLOW,
                sys_epoll_create1: ALLOW,
                sys_epoll_ctl: ALLOW,
                sys_epoll_wait: ALLOW,
                sys_epoll_pwait: ALLOW,
                sys_timerfd_settime: ALLOW,
                sys_eventfd2: ALLOW,
                sys_waitid: ALLOW,
                sys_wait4: ALLOW,
                # Network system calls, we don't sandbox these
                sys_socket: ALLOW,
                sys_socketpair: ALLOW,
                sys_connect: ALLOW,
                sys_setsockopt: ALLOW,
                sys_getsockname: ALLOW,
                sys_sendmmsg: ALLOW,
                sys_recvfrom: ALLOW,
                sys_sendto: ALLOW,
                # Miscellaneous other system calls
                sys_msync: ALLOW,
                sys_clock_nanosleep: ALLOW,
                sys_memfd_create: ALLOW,
                sys_rt_sigsuspend: ALLOW,
            }
        )

        # FreeBSD-specific syscalls
        if 'freebsd' in sys.platform:
            self.update(
                {
                    sys_rfork: ALLOW,
                    sys_procctl: ALLOW,
                    sys_cap_rights_limit: ALLOW,
                    sys_posix_fadvise: ALLOW,
                    sys_posix_fallocate: ALLOW,
                    sys_setrlimit: ALLOW,
                    sys_cap_ioctls_limit: ALLOW,
                    sys_cap_fcntls_limit: ALLOW,
                    sys_cap_enter: ALLOW,
                    sys_utimes: self.check_file_access('utimes', 0),
                }
            )

    def do_utimensat(self, debugger: AdvancedDebugger) -> bool:
        timespec = struct.Struct({32: '=ii', 64: '=QQ'}[debugger.address_bits])

        # Emulate https://github.com/torvalds/linux/blob/v5.14/fs/utimes.c#L152-L161
        times_ptr = debugger.uarg2
        if times_ptr:
            try:
                buffer = debugger.readbytes(times_ptr, timespec.size * 2)
            except OSError:
                return ACCESS_EFAULT(debugger)

            times = list(timespec.iter_unpack(buffer))
            if times[0][1] == UTIME_OMIT and times[1][1] == UTIME_OMIT:
                debugger.syscall = -1

                def on_return():
                    debugger.result = 0

                debugger.on_return(on_return)
                return True

        # Emulate https://github.com/torvalds/linux/blob/v5.14/fs/utimes.c#L142-L143
        if debugger.uarg0 != AT_FDCWD and not debugger.uarg1:
            path = self._getfd_pid(debugger.tid, debugger.uarg0)
            return True if self.write_fs_jail.check(path) else ACCESS_EPERM(debugger)

        return self.check_file_access_at('utimensat')(debugger)

    def do_fchmod(self, debugger: Debugger) -> bool:
        path = self._getfd_pid(debugger.tid, debugger.uarg0)
        return True if self.write_fs_jail.check(path) else ACCESS_EPERM(debugger)

    def do_rename(self, debugger: Debugger) -> bool:
        old_path, old_path_error = self.read_path('rename', debugger, debugger.uarg0)
        if old_path_error is not None:
            return old_path_error

        new_path, new_path_error = self.read_path('rename', debugger, debugger.uarg1)
        if new_path_error is not None:
            return new_path_error

        if not self._file_access_check(old_path, debugger, is_write=True, is_open=False):
            return ACCESS_EPERM(debugger)
        if not self._file_access_check(new_path, debugger, is_write=True, is_open=False):
            return ACCESS_EPERM(debugger)

        return True

    def do_renameat(self, debugger: Debugger) -> bool:
        old_path, old_path_error = self.read_path('renameat', debugger, debugger.uarg1)
        if old_path_error is not None:
            return old_path_error

        new_path, new_path_error = self.read_path('renameat', debugger, debugger.uarg3)
        if new_path_error is not None:
            return new_path_error

        if not self._file_access_check(old_path, debugger, is_write=True, is_open=False, dirfd=debugger.uarg0):
            return ACCESS_EPERM(debugger)
        if not self._file_access_check(new_path, debugger, is_write=True, is_open=False, dirfd=debugger.uarg2):
            return ACCESS_EPERM(debugger)

        return True


class CompiledExecutor(BaseExecutor, metaclass=_CompiledExecutorMeta):
    executable_size = env.compiler_size_limit * 1024
    compiler_time_limit = env.compiler_time_limit
    compile_output_index = 1

    is_cached = False
    warning: Optional[bytes] = None
    _executable: Optional[str] = None
    _code: Optional[str] = None

    compiler_read_fs: Sequence[FilesystemAccessRule] = []
    compiler_write_fs: Sequence[FilesystemAccessRule] = []

    def __init__(self, problem_id: str, source_code: bytes, *args, **kwargs) -> None:
        super().__init__(problem_id, source_code, **kwargs)
        self.warning = None
        self._executable = None

    def cleanup(self) -> None:
        if not self.is_cached:
            super().cleanup()

    def create_files(self, problem_id: str, source_code: bytes, *args, **kwargs) -> None:
        self._code = self._file(self.source_filename_format.format(problem_id=problem_id, ext=self.ext))
        with open(self._code, 'wb') as fo:
            fo.write(utf8bytes(source_code))

    def get_compile_args(self) -> List[str]:
        raise NotImplementedError()

    def get_compile_env(self) -> Optional[Dict[str, str]]:
        return None

    def get_compile_popen_kwargs(self) -> Dict[str, Any]:
        return {}

    def create_compile_process(self, args: List[str]) -> TracedPopen:
        # Some languages may insist on providing certain functionality (e.g. colored highlighting of errors) if they
        # feel they are connected to a terminal. Some are more persistent than others in enforcing this, so this hack
        # aims to provide a convincing-enough lie to the runtime so that it starts singing in color.
        #
        # Emulate the streams of a process connected to a terminal: stdin, stdout, and stderr are all ptys.
        _master, _slave = pty.openpty()
        # Some runtimes *cough cough* Swift *cough cough* actually check the environment variables too.
        env = self.get_compile_env() or os.environ.copy()
        env['TERM'] = 'xterm'
        # Instruct compilers to put their temporary files into the submission directory,
        # so that we can allow it as writeable, rather than of all of /tmp.
        assert self._dir is not None
        env['TMPDIR'] = self._dir

        proc = TracedPopen(
            [utf8bytes(a) for a in args],
            **{
                'executable': utf8bytes(args[0]),
                'security': CompilerIsolateTracer(self._dir, self.compiler_read_fs, self.compiler_write_fs),
                'stderr': _slave,
                'stdout': _slave,
                'stdin': _slave,
                'cwd': utf8bytes(self._dir),
                'env': env,
                'nproc': -1,
                'fsize': self.executable_size,
                'time': self.compiler_time_limit or 0,
                'memory': 0,
                **self.get_compile_popen_kwargs(),
            }
        )

        class io_error_wrapper:
            """
            Wrap pty-related IO errors so that we don't crash Popen.communicate()
            """

            def __init__(self, io: IO) -> None:
                self.io = io

            def read(self, *args, **kwargs):
                try:
                    return self.io.read(*args, **kwargs)
                except (IOError, OSError):
                    return b''

            def __getattr__(self, attr):
                return getattr(self.io, attr)

        # Since stderr and stdout are connected to the same slave pty, proc.stderr will contain the merged stdout
        # of the process as well.
        proc.stderr = io_error_wrapper(os.fdopen(_master, 'rb'))  # type: ignore

        os.close(_slave)
        return proc

    def get_compile_output(self, process: TracedPopen) -> bytes:
        # Use safe_communicate because otherwise, malicious submissions can cause a compiler
        # to output hundreds of megabytes of data as output before being killed by the time limit,
        # which effectively murders the MySQL database waiting on the site server.
        limit = env.compiler_output_character_limit
        try:
            output = safe_communicate(process, None, outlimit=limit, errlimit=limit)[self.compile_output_index]
        except OutputLimitExceeded:
            output = b'compiler output too long (> 64kb)'

        if self.is_failed_compile(process):
            if process.is_tle:
                output = b'compiler timed out (> %d seconds)' % self.compiler_time_limit
            if process.protection_fault:
                print_protection_fault(process.protection_fault)
            self.handle_compile_error(output)

        return output

    def get_compiled_file(self) -> str:
        return self._file(self.problem)

    def is_failed_compile(self, process: TracedPopen) -> bool:
        return process.returncode != 0

    def handle_compile_error(self, output: bytes) -> None:
        raise CompileError(output)

    def get_binary_cache_key(self) -> bytes:
        return utf8bytes(self.problem) + self.source

    def compile(self) -> str:
        process = self.create_compile_process(self.get_compile_args())
        self.warning = self.get_compile_output(process)
        self._executable = self.get_compiled_file()
        return self._executable

    def get_cmdline(self, **kwargs) -> List[str]:
        return [self.problem]

    def get_executable(self) -> str:
        assert self._executable is not None
        return self._executable
