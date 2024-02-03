import os
import logging


def logger_init(filename, folder='logs', mode='a', loglevel='INFO'):
    '''
    init logger
    loglevel examples: ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL]
    '''

    # Create logs folder if not exists
    if not os.path.exists(folder):
        os.makedirs(folder)

    # Create logger
    logger = logging.getLogger('logger')

    # Create formatter
    formatter = logging.Formatter('[%(asctime)s] %(levelname)s - #%(lineno)d = %(filename)s -> %(message)s', 
                                  datefmt='%Y-%m-%d %H:%M:%S')
    
    # Create file handler
    fh = logging.FileHandler(encoding='utf-8', filename=f"{folder}/{filename}", mode=mode)

    # Create console handler
    ch = logging.StreamHandler()

    # Add formatter
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    # Add handler to logger
    logger.addHandler(fh)
    logger.addHandler(ch)

    # Set loglevel
    if loglevel.upper() == 'DEBUG':
        logger.setLevel(logging.DEBUG)
        ch.setLevel(logging.DEBUG)
    elif loglevel.upper() == 'INFO':
        logger.setLevel(logging.INFO)
        ch.setLevel(logging.INFO)
    elif loglevel.upper() == 'WARNING':
        logger.setLevel(logging.WARNING)
        ch.setLevel(logging.WARNING)
    elif loglevel.upper() == 'ERROR':
        logger.setLevel(logging.ERROR)
        ch.setLevel(logging.ERROR)
    elif loglevel.upper() == 'CRITICAL':
        logger.setLevel(logging.CRITICAL)
        ch.setLevel(logging.CRITICAL)
    else:
        logger.setLevel(logging.INFO)
        ch.setLevel(logging.INFO)

    return logger

if __name__ == '__main__':
    # Test logging
    log = logger_init('testing.log', 'logs', 'a')
    log.debug('debug')
    log.info('info')
    log.warning('warning')
    log.error('error')
    log.critical('critical')
    try:
        a = 3/0
    except Exception:
        log.exception('exception')