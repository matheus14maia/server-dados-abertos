from flask import Flask, jsonify, request
from flask_cors import CORS
import sqlite3

app = Flask(__name__)
CORS(app)  # This will enable CORS for all routes

DATABASE = 'C:\\Ficus\\Dados Abertos\\dbDadosAbertos.db'


def query_db(query, args=(), one=False):
    con = sqlite3.connect(DATABASE)
    cur = con.cursor()
    cur.execute(query, args)
    rv = cur.fetchall()
    cur.close()
    con.close()
    return (rv[0] if rv else None) if one else rv


@app.route('/cnaes', methods=['GET'])
def get_cnaes():
    page = int(request.args.get('page', 1))
    limit = int(request.args.get('limit', 200))
    offset = (page - 1) * limit
    query = 'SELECT * FROM Cnaes'
    filters = []

    for key in request.args:
        if key not in ['page', 'limit']:
            filters.append(f"{key} = ?")

    if filters:
        query += ' WHERE ' + ' AND '.join(filters)

    query += f' LIMIT {limit} OFFSET {offset}'
    rows = query_db(query, tuple(request.args.get(key) for key in request.args if key not in ['page', 'limit']))
    cnaes = [{"ID": row[0], "codigo": row[1], "descricao": row[2]} for row in rows]
    return jsonify(cnaes)


@app.route('/empresas', methods=['GET'])
def get_empresas():
    page = int(request.args.get('page', 1))
    limit = int(request.args.get('limit', 200))
    offset = (page - 1) * limit
    query = 'SELECT * FROM Empresas'
    filters = []

    for key in request.args:
        if key not in ['page', 'limit']:
            filters.append(f"{key} = ?")

    if filters:
        query += ' WHERE ' + ' AND '.join(filters)

    query += f' LIMIT {limit} OFFSET {offset}'
    rows = query_db(query, tuple(request.args.get(key) for key in request.args if key not in ['page', 'limit']))
    empresas = [
        {
            "ID": row[0],
            "cnpj_basico": row[1],
            "nome": row[2],
            "natureza": row[3],
            "qualificacao_do_responsavel": row[4],
            "capital": row[5],
            "porte": row[6],
            "ente_responsavel": row[7]
        }
        for row in rows
    ]
    return jsonify(empresas)


@app.route('/estabelecimentos', methods=['GET'])
def get_estabelecimentos():
    page = int(request.args.get('page', 1))
    limit = int(request.args.get('limit', 200))
    offset = (page - 1) * limit
    query = 'SELECT * FROM Estabelecimentos'
    filters = []

    for key in request.args:
        if key not in ['page', 'limit']:
            filters.append(f"{key} = ?")

    if filters:
        query += ' WHERE ' + ' AND '.join(filters)

    query += f' LIMIT {limit} OFFSET {offset}'
    rows = query_db(query, tuple(request.args.get(key) for key in request.args if key not in ['page', 'limit']))
    estabelecimentos = [
        {
            "ID": row[0],
            "cnpj_basico": row[1],
            "cnpj_ordem": row[2],
            "cnpj_dv": row[3],
            "identificador": row[4],
            "nome": row[5],
            "situacao": row[6],
            "data_situacao": row[7],
            "motivo_situacao": row[8],
            "nome_exterior": row[9],
            "pais": row[10],
            "data_inicio": row[11],
            "cnae_principal": row[12],
            "cnae_secundaria": row[13],
            "tipo_logradouro": row[14],
            "logradouro": row[15],
            "numero": row[16],
            "complemento": row[17],
            "bairro": row[18],
            "CEP": row[19],
            "UF": row[20],
            "Municipio": row[21],
            "DDD_Telefone": row[22],
            "telefone": row[23],
            "DDD_Celular": row[24],
            "celular": row[25],
            "DDD_Fax": row[26],
            "Fax": row[27],
            "correio_eletronico": row[28],
            "situacao_especial": row[29],
            "data_sit_especial": row[30]
        }
        for row in rows
    ]
    return jsonify(estabelecimentos)


@app.route('/estoqueProcessos', methods=['GET'])
def get_estoqueProcessos():
    page = int(request.args.get('page', 1))
    limit = int(request.args.get('limit', 200))
    offset = (page - 1) * limit
    query = f'SELECT * FROM EstoqueProcessos'
    filters = []

    for key in request.args:
        if key not in ['page', 'limit']:
            filters.append(f"{key} = ?")

    if filters:
        query += ' WHERE ' + ' AND '.join(filters)

    query += f' LIMIT {limit} OFFSET {offset}'
    rows = query_db(query, tuple(request.args.get(key) for key in request.args if key not in ['page', 'limit']))
    estoqueProcessos = [
        {
            "ID": row[0],
            "numero_procesoo": row[1],
            "data_protocolo": row[2],
            "questionamento_primario": row[3],
            "questionamento_secundario": row[4],
            "tipo_contribuinte": row[5],
            "tributo": row[6],
            "fase_processual": row[7],
            "instancia": row[8],
            "data_entrada_carf": row[9]
        }
        for row in rows
    ]
    return jsonify(estoqueProcessos)


@app.route('/paises', methods=['GET'])
def get_paises():
    page = int(request.args.get('page', 1))
    limit = int(request.args.get('limit', 200))
    offset = (page - 1) * limit
    query = f'SELECT * FROM Paises'
    filters = []

    for key in request.args:
        if key not in ['page', 'limit']:
            filters.append(f"{key} = ?")

    if filters:
        query += ' WHERE ' + ' AND '.join(filters)

    query += f' LIMIT {limit} OFFSET {offset}'
    rows = query_db(query, tuple(request.args.get(key) for key in request.args if key not in ['page', 'limit']))
    paises = [{"ID": row[0], "codigo": row[1], "descricao": row[2]} for row in rows]
    return jsonify(paises)


@app.route('/municipios', methods=['GET'])
def get_municipios():
    page = int(request.args.get('page', 1))
    limit = int(request.args.get('limit', 200))
    offset = (page - 1) * limit
    query = f'SELECT * FROM Municipios'
    filters = []

    for key in request.args:
        if key not in ['page', 'limit']:
            filters.append(f"{key} = ?")

    if filters:
        query += ' WHERE ' + ' AND '.join(filters)

    query += f' LIMIT {limit} OFFSET {offset}'
    rows = query_db(query, tuple(request.args.get(key) for key in request.args if key not in ['page', 'limit']))
    municipios = [{"ID": row[0], "codigo": row[1], "descricao": row[2]} for row in rows]
    return jsonify(municipios)


@app.route('/lucroArbitrado', methods=['GET'])
def get_lucroArbitrado():
    page = int(request.args.get('page', 1))
    limit = int(request.args.get('limit', 200))
    offset = (page - 1) * limit
    query = f'SELECT * FROM LucroArbitrado'
    filters = []

    for key in request.args:
        if key not in ['page', 'limit']:
            filters.append(f"{key} = ?")

    if filters:
        query += ' WHERE ' + ' AND '.join(filters)

    query += f' LIMIT {limit} OFFSET {offset}'
    rows = query_db(query, tuple(request.args.get(key) for key in request.args if key not in ['page', 'limit']))
    lucroArbitrado = [
        {
            "ID": row[0],
            "ano": row[1],
            "cnpj": row[2],
            "cnpj_da_scp": row[3],
            "forma_de_tributacao": row[4],
            "quantidade_de_escrituracoes": row[5]
        }
        for row in rows
    ]
    return jsonify(lucroArbitrado)


@app.route('/lucroPresumido', methods=['GET'])
def get_lucroPresumido():
    page = int(request.args.get('page', 1))
    limit = int(request.args.get('limit', 200))
    offset = (page - 1) * limit
    query = f'SELECT * FROM LucroPresumido'
    filters = []

    for key in request.args:
        if key not in ['page', 'limit']:
            filters.append(f"{key} = ?")

    if filters:
        query += ' WHERE ' + ' AND '.join(filters)

    query += f' LIMIT {limit} OFFSET {offset}'
    rows = query_db(query, tuple(request.args.get(key) for key in request.args if key not in ['page', 'limit']))
    lucroPresumido = [
        {
            "ID": row[0],
            "ano": row[1],
            "cnpj": row[2],
            "cnpj_da_scp": row[3],
            "forma_de_tributacao": row[4],
            "quantidade_de_escrituracoes": row[5]
        }
        for row in rows
    ]
    return jsonify(lucroPresumido)


@app.route('/lucroReal', methods=['GET'])
def get_lucroReal():
    page = int(request.args.get('page', 1))
    limit = int(request.args.get('limit', 200))
    offset = (page - 1) * limit
    query = f'SELECT * FROM LucroReal'
    filters = []

    for key in request.args:
        if key not in ['page', 'limit']:
            filters.append(f"{key} = ?")

    if filters:
        query += ' WHERE ' + ' AND '.join(filters)

    query += f' LIMIT {limit} OFFSET {offset}'
    rows = query_db(query, tuple(request.args.get(key) for key in request.args if key not in ['page', 'limit']))
    lucroReal = [
        {
            "ID": row[0],
            "ano": row[1],
            "cnpj": row[2],
            "cnpj_da_scp": row[3],
            "forma_de_tributacao": row[4],
            "quantidade_de_escrituracoes": row[5]
        }
        for row in rows
    ]
    return jsonify(lucroReal)


if __name__ == '__main__':
    app.run(debug=True)
