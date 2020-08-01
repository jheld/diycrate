import subprocess


def notify_user_with_gui(message, crate_logger=None, expire_time=None):
    """
    Sends the message to the user
    :param message:
    :param crate_logger:
    :param expire_time:
    :return:
    """
    notify_options = []
    # apparently notify-send does not universally use expire-time :( docs :(
    # if expire_time is not None:
    #     notify_options.append("--expire-time={expire_time}".format(expire_time=expire_time))
    notify_options.append(message)
    proc = subprocess.Popen(["notify-send"] + notify_options)
    if proc.returncode:
        if crate_logger:
            crate_logger.debug(
                "Tried sending a message to user, return code: {}".format(
                    proc.returncode
                )
            )
