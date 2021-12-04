from pathlib import Path

def get_user_history_path():
    history_path = Path("~/.config/repoman/.cli_history").expanduser()
    if not history_path.exists() or not history_path.is_file():
        open(history_path, "a").close()
    return history_path
