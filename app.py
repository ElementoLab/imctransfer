"""
Tiny app to receive the OAuth code from imctools.
"""

from flask import Flask, request

app = Flask(__name__)


@app.route("/", methods=["GET"])
def home():
    code = request.args.get("code")
    if code is None:
        return "Request incomplete."
    return (
        f"<p>Here is the authentication code:<br>{code}</p>"
        + "<p>Please paste it in the CLI.</p>"
    )


if __name__ == "__main__":
    app.run()
