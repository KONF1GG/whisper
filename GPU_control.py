import fcntl
import time
import os
import logging
from contextlib import contextmanager


logger = logging.getLogger("gpu-lock")


class GPULock:
    def __init__(self, lock_file_path="/shared/gpu.lock", timeout=None):
        """
        Инициализация менеджера блокировки GPU.
        :param lock_file_path: Путь к файлу блокировки
        :param timeout: Максимальное время ожидания в секундах (None = бесконечно)
        """
        self.lock_file_path = lock_file_path
        self.timeout = timeout
        self.lock_file = None

    def acquire(self):
        """Попытка установить блокировку"""
        start_time = time.time()
        self.lock_file = open(self.lock_file_path, "a")
        while True:
            try:
                fcntl.flock(self.lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
                logger.info("[%s] Блокировка GPU установлена", os.getpid())
                return True
            except IOError:
                if (
                    self.timeout is not None
                    and (time.time() - start_time) > self.timeout
                ):
                    logger.warning("[%s] Таймаут ожидания GPU истек", os.getpid())
                    return False
                logger.info("[%s] GPU занят, жду...", os.getpid())
                time.sleep(1)

    def release(self):
        """Снятие блокировки"""
        if self.lock_file:
            fcntl.flock(self.lock_file, fcntl.LOCK_UN)
            self.lock_file.close()
            logger.info("[%s] Блокировка GPU снята", os.getpid())

    def __enter__(self):
        """Вход в контекст"""
        if not self.acquire():
            raise RuntimeError("Не удалось захватить GPU")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Выход из контекста"""
        self.release()


@contextmanager
def gpu_lock(timeout=None):
    lock = GPULock(timeout=timeout)
    with lock:
        yield
