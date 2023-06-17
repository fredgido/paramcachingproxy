import signal

mutable = dict(is_requested = False)


def handle_keyboard_interrupt(signal, frame):
    print("Keyboard interrupt detected!")
    mutable["is_requested"] = True

# Register the handler for the SIGINT signal (Ctrl+C)
signal.signal(signal.SIGINT, handle_keyboard_interrupt)