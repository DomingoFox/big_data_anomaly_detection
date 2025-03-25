import logging
import os

class Logger:
    _instance = None

    def __new__(cls, output_dir=None, verbose_timing=False):
        if cls._instance is None:
            cls._instance = super(Logger, cls).__new__(cls)
            cls._instance._initialize(output_dir, verbose_timing)
        return cls._instance

    def _initialize(self, output_dir=None, verbose_timing=False):
        self.logger = logging.getLogger('ApplicationLogger')
        self.logger.setLevel(logging.INFO)
        self.verbose_timing = verbose_timing  # Control timing logs verbosity

        if not self.logger.handlers:
            ch = logging.StreamHandler()
            ch.setLevel(logging.INFO)
            ch.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
            self.logger.addHandler(ch)

            if output_dir:
                log_file = os.path.join(output_dir, 'application.log')
                fh = logging.FileHandler(log_file)
                fh.setLevel(logging.INFO)
                fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
                self.logger.addHandler(fh)

    def info(self, message, category="status"):
        if category == "timing" and not self.verbose_timing:
            return
        self.logger.info(message)

    def error(self, message):
        self.logger.error(message)

    def debug(self, message):
        self.logger.debug(message)