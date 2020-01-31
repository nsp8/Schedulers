from flask import Flask, render_template
from Timer import aps_test

app = Flask(__name__)


@app.route('/')
def index():
    timer_msg = aps_test.timer()
    return render_template('index.html', timer_msg=timer_msg)


if __name__ == '__main__':
    app.run(debug=True)
