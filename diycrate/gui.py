import subprocess


def notify_user_with_gui(message, crate_logger=None):
    """
    Sends the message to the user
    :param message:
    :param crate_logger:
    :return:
    """
    proc = subprocess.Popen(['notify-send', message])
    if proc.returncode:
        if crate_logger:
            crate_logger.debug('Tried sending a message to user, return code: {}'.format(proc.returncode))
