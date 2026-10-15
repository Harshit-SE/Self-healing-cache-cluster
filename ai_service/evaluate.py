"""Train and save the hot-key model and its time-split evaluation report."""

import json

from hotkey_advisor import load_model


if __name__ == "__main__":
    _model, report = load_model()
    print(json.dumps(report, indent=2))
