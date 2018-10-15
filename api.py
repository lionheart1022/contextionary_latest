from flask import Flask
from flask import jsonify
from contextionaryDatabase import Database
import config
from DatabaseAdapter import connectHost, connectDB

app = Flask(__name__)

db = Database()
connectDB = connectDB()


@app.route('/', methods=['GET'])
def index():
    """
    index endpoint
    """
    return 'Please go to api endpoint to read text. ' \
           'When you input text in endpoint, you have to replace space with _.'


@app.route('/<text>', methods=['GET'])
def display(text):
    if '_' in text:
        text = text.replace('_', ' ')

    db.inputText.addRecord(text, connectDB)

    cur = connectDB.connection.cursor()

    strSQL = 'SELECT * FROM input_text ORDER BY input_text_id'
    cur.execute(strSQL)
    input_text_id = None
    input_id_list = cur.fetchall()

    for input_id in input_id_list:
        if input_id[1] == text:
            input_text_id = input_id[0]
            print(input_text_id)

    if input_text_id:
        strSQL = 'SELECT * FROM input_text_keywords WHERE input_text_id={}'.format(input_text_id)
        cur.execute(strSQL)
        result_keyword = cur.fetchall()

        strSQL = 'SELECT * FROM input_text_context_identifier WHERE input_text_id={}'.format(input_text_id)
        cur.execute(strSQL)
        result_identifier = cur.fetchall()

        result = {'input_text_keywords': [{"input_text_id": keyword[0], "context_id": keyword[1],
                                           "keyword_id": keyword[2], "keyword_position": keyword[3],
                                           "keyword_text": keyword[4], "phrase_id": keyword[5]}
                                          for keyword in result_keyword],
                  'input_text_context_identifier': [{"input_text_id": identifier[0], "context_id": identifier[1],
                                                     "context_weight": identifier[2]}
                                                    for identifier in result_identifier]}

        return jsonify(result)


if __name__ == '__main__':
    app.run(debug=True)
