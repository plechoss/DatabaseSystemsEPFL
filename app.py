from flask import Flask, render_template, json, request
import psycopg2

app = Flask(__name__)

def executesqlcommand(cmdSql, args):
    con = None
    try:
        dbName = "test_python"
        userName = "ginger"
        con = psycopg2.connect("dbname='" + dbName + "' user='" + userName +"'")
        cur = con.cursor()
        cur.execute(cmdSql, args)
        con.commit()
    except psycopg2.DatabaseError, e:
        if con:
            con.rollback()
    finally:
        if con:
            con.close()
def convert(name):
    if(name.endswith('languagecode')):
        return 'lcode'
    elif(name.endswith('code')):
        return 'code'
    elif(name.endswith('name')):
        return 'name'
    elif(name.endswith('languageid')):
        return 'lid'
    elif(name.endswith('indicaid')):
        return 'ipid'
    elif(name.endswith('personid') or name.endswith('publisherid')):
        return 'pid'
    elif(name.endswith('targetstoryid')):
        return 'target_sid'
    elif(name.endswith('originstoryid')):
        return 'origin_sid'
    elif(name.endswith('storyid') or name.endswith('seriesid')):
        return 'sid'
    elif(name.endswith('targetissueid')):
        return 'target_iid'
    elif(name.endswith('originalissueid')):
        return 'origin_iid'
    elif(name.endswith('issueid')):
        return 'iid'
    elif(name.endswith('storytypeid')):
        return 'stid'
    elif(name.endswith('serpubtypeid')):
        return 'spid'
    elif(name.endswith('countryid') or name.endswith('characterid')):
        return 'cid'
    elif(name.endswith('brandid')):
        return 'bgid'
    elif(name.endswith('indicaid')):
        return 'ipid'
    elif(name.endswith('actionid')):
        return 'aid'
    elif(name.endswith('featureid')):
        return 'fid'
    elif(name.endswith('msggenreid') or name.endswith('storygenreid')):
        return 'sgid'
    elif(name.endswith('genreid')):
        return 'gid'
    elif(name.endswith('reprintid')):
        return 'rid'
    elif(name.endswith('firstid')):
        return 'fiid'
    elif(name.endswith('lastid')):
        return 'liid'
    elif(name.endswith('details')):
        return 'details'
    elif(name.endswith('reprintnotes')):
        return 'reprint_notes'
    elif(name.endswith('notes')):
        return 'notes'
    elif(name.endswith('format')):
        return 'format'
    elif(name.endswith('began')):
        return 'year_began'
    elif(name.endswith('ended')):
        return 'year_ended'
    elif(name.endswith('title')):
        return 'title'
    elif(name.endswith('url')):
        return 'url'
    elif(name.endswith('indiciasurrogate')):
        return 'is_surrogate'
    else:
        return name


@app.route('/')
def main():
    return render_template('index.html')

@app.route('/insert', methods=['POST'])
def insert():
    #read the values from the form
    
    _table = request.form['searchtables']
    if(_table == 'language'):
        _id = request.form['languageid']
        _code = request.form['languagecode']
        _name = request.form['languagename']
        cmdSql = "INSERT INTO languages(lid, lcode, name) VALUES(%s, %s, %s)"
        executesqlcommand(cmdSql, (_id, _code, _name))
    
    elif(_table == 'country'):
        _id = request.form['countryid']
        _code = request.form['countrycode']
        _name = request.form['countryname']
        cmdSql = "INSERT INTO countries(cid, ccode, name) VALUES(%s, %s, %s)"
        executesqlcommand(cmdSql, (_id, _code, _name))

    elif(_table == 'serpubtype'):
        _id = request.form['serpubtypeid']
        _name = request.form['serpubtypename']
        cmdSql = "INSERT INTO seriespublicationtype(spid, name) VALUES(%s, %s)"
        executesqlcommand(cmdSql, (_id, _name))

    elif(_table == 'storytype'):
        _id = request.form['storytypeid']
        _name = request.form['storytypename']
        cmdSql = "INSERT INTO storytype(stid, name) VALUES(%s, %s)"
        executesqlcommand(cmdSql, (_id, _name))

    elif(_table == 'storygenre'):
        _id = request.form['storygenreid']
        _name = request.form['storygenrename']
        cmdSql = "INSERT INTO storygenre(stid, name) VALUES(%s, %s)"
        executesqlcommand(cmdSql, (_id, _name))

    elif(_table == 'storyfeature'):
        _id = request.form['storyfeatureid']
        _name = request.form['storyfeaturename']
        cmdSql = "INSERT INTO storyfeature(fid, name) VALUES(%s, %s)"
        executesqlcommand(cmdSql, (_id, _name))

    elif(_table == 'character'):
        _id = request.form['characterid']
        _name = request.form['charactername']
        cmdSql = "INSERT INTO characters(cid, name) VALUES(%s, %s)"
        executesqlcommand(cmdSql, (_id, _name))

    elif(_table == 'publisher'):
        _id = request.form['publisherid']
        _name = request.form['publishername']
        _countryid = request.form['publishercountryid']
        _began = request.form['publisherbegan']
        if(_began == "NULL"):
            _began = None
        _ended = request.form['publisherended']
        if(_ended == "NULL"):
            _ended = None
        _notes = request.form['publishernotes']
        _url = request.form['publisherurl']
        cmdSql = "INSERT INTO publishers(pid, name, cid, year_began, year_ended, notes, url) VALUES(%s, %s, %s, %s, %s, %s, %s)"
        executesqlcommand(cmdSql, (_id, _name, _countryid, _began, _ended, _notes, _url))
    
    elif(_table == 'indiciapublisher'):
        _id = request.form['indiciaid']
        _name = request.form['indicianame']
        _indiciaid = request.form['indiciapubid']
        _countryid = request.form['indiciacountryid']
        _began = request.form['indiciabegan']
        if(_began == "NULL"):
            _began = None
        _ended = request.form['indiciaended']
        if(_ended == "NULL"):
            _ended = None
        _surrrogate = request.form['indiciasurrogate']
        if(_surrogate==0):
            _surrogate = False
        else:
            _surrogate = True
        _notes = request.form['indicianotes']
        _url = request.form['indiciaurl']
        cmdSql = "INSERT INTO indiciapublishers(ipid, name, pid, cid, year_began, year_ended, is_surrogate, notes, url) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        executesqlcommand(cmdSql, (_id, _name, _indiciaid, _countryid, _began, _ended, _surrogate, _notes, _url))
    else:
        _id = request.form['brandid']
        _name = request.form['brandname']
        _began = request.form['brandbegan']
        if(_began == "NULL"):
            _began = None
        _ended = request.form['brandended']
        if(_ended == "NULL"):
            _ended = None
        _notes = request.form['brandnotes']
        _url = request.form['brandurl']
        cmdSql = "INSERT INTO brandgroups(bgid, name, year_began, year_ended, notes, url, pid) VALUES(%s, %s, %s, %s, %s, %s, %s)"
        executesqlcommand(cmdSql, (_id, _name, _began, _notes, _url))
"""
    if _name and _id:
        return json.dumps({'html':'<span>All fields good !!</span>'})
    else:
        return json.dumps({'html':'<span>Enter the required fields</span>'})
"""
@app.route('/search', methods=['POST'])
def search():
    _table = request.form['searchtables']
    searchcmd = "SELECT * FROM " + _table + " WHERE "
    for fieldname, value in field.data.items():
        pass

    for field in form:
        if(field.data != "NULL"):
            tmpname = convert(field.name)
            searchcmd = searchcmd + tmpname + "=" + str(field.data) + " AND "
    searchcmd = searchcmd[:-5]
    query = executesqlcommand(searchcmd, None)
    print query

if __name__ == "__main__":
    app.run()
