from flask import Flask, render_template, request
app = Flask(__name__)

@app.route("/")
def main():
    return render_template('main.html')

@app.route("/user")
def user():
    return render_template('user.html')

@app.route("/assignment")
def assignment():
    system = request.args.get('system', '')
    cqs = [{'id': 15, 'sql': 'SELECT 1', 'projs': 3, 'results': [('1', '2', '3')]}, {'id': 20, 'sql': 'SELECT 2', 'projs': 3, 'results': [('4', '5', '6')]}]
    tq = {'projs': 3}
    return render_template('assignment.html', system=system, cqs=cqs, tq=tq)
