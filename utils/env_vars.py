import os


def is_flight_mode_on() -> bool:
    return is_env_var_on('FLIGHTMODE')


def is_debug_mode_on() -> bool:
    return is_env_var_on('DEBUG')


def is_env_var_on(env_var) -> bool:
    if os.getenv(env_var) == '1':
        print(env_var, ' is on!')
        return True

    return False
