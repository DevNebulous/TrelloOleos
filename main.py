import pyodbc, sqlite3, requests
from datetime import datetime
import sys
# Variaveis super

AreaDeTrabalho = '##'
Quadro = '##'
list20D = '##'
list10D = '##'
list05D = '##'
list24H = '##'
listAtrasados = '##'
listCompletos = '##'
listValidados = '##'
listFinalizados = '##'
key = "##"
token = "##"


def conectToSQL ():
    # Substitua essas informações pelos detalhes do seu banco de dados
    server = '#####'
    database = '#####'
    username = '######'
    password = '######'

    # Construa a string de conexão
    connection_string = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

    # Tente estabelecer a conexão
    try:
        conn = pyodbc.connect(connection_string)
        # print("Conexão bem-sucedida!")

        # Agora você pode executar consultas, inserções, atualizações, etc.

        # Exemplo de consulta
        cursor = conn.cursor()
        cursor.execute("""


    SELECT
    ServicoExecutado.Oid,
    solicitacaoServico.NumeroSolicitacao,
    SolicitacaoServico.OrcamentoCRM,
    Protocolo.Identificacao as Triagem,
    ProtocoloAmostra.Identificacao,
    obj2.Identificacao as NomeEnsaio,
    Cliente.Nome as NomeCliente,
    ObjetoAmostravelTransformador.Tag as TagTrafo,
    ObjetoAmostravel.Identificacao as NumeroSerieTrafo,
    ServicoExecutado.DataInicialAnalise as DataInicioServicoExecutavel,
    ServicoExecutado.DataFinalAnalise as DataFinalServicoExecutavel,
    ProgramacaoServicosAgenda.StartOn as DataInicioProgramacao,
    ProgramacaoServicosAgenda.EndOn as DataFinalProgramacao,
    ServicoExecutado.Situacao,
    ServicoExecutado.Parecer,
    Protocolo.Data as DataInformadaProtocolo,
    Protocolo.ApprovedOn as DataAprovadoProtocolo,
    LAB.Nome as Laboratorio,
    MatrizOuTipo.Nome as Matriz,
    ProtocoloAmostra.DataChegadaAmostra,
    ObjetoSolicitado.DataProgFimCalculada
    FROM
    ServicoExecutado

    INNER JOIN ExecucaoServico ON ServicoExecutado.ExecucaoServico = ExecucaoServico.Oid
    INNER JOIN AmostraObjeto ON ExecucaoServico.Amostra = AmostraObjeto.Oid
    INNER JOIN ProtocoloAmostra ON ProtocoloAmostra.Oid=AmostraObjeto.ProtocoloAmostra
    INNER JOIN Protocolo ON ProtocoloAmostra.Protocolo=Protocolo.oid
    LEFT JOIN ObjetoAmostravelTransformador ON ProtocoloAmostra.ObjetoTransformador=ObjetoAmostravelTransformador.Oid
    LEFT JOIN ObjetoAmostravel ON ObjetoAmostravelTransformador.Oid = ObjetoAmostravel.Oid
    INNER JOIN ObjetoSolicitado ON AmostraObjeto.ObjetoSolicitado=ObjetoSolicitado.oid

    INNER JOIN MatrizOuTipo ON ProtocoloAmostra.MatrizOuTipo=MatrizOuTipo.Oid
    INNER JOIN ObjetoAmostravel obj2 ON AmostraObjeto.Objeto = obj2.Oid
    INNER JOIN SolicitacaoServico ON Protocolo.SolicitacaEnsaio=SolicitacaoServico.Oid
    INNER JOIN Cliente ON SolicitacaoServico.AreaSolicitante=Cliente.Oid
    LEFT JOIN ProgramacaoServicosAgenda ON ProgramacaoServicosAgenda.Oid=ServicoExecutado.Programacao
    INNER JOIN Cliente lab ON ExecucaoServico.ObjectOwner=LAB.Oid

    WHERE ExecucaoServico.GCRecord IS NULL AND ServicoExecutado.Situacao NOT IN (2,3,4,5) and ServicoExecutado.GCRecord is null
          and ProtocoloAmostra.Identificacao is not null
          and Lab.nome='Lab. de Óleos' AND
          Protocolo.CreatedOn >= '2023-01-01'


        """)

        rows = cursor.fetchall()


        for row in rows:
            conn2 = sqlite3.connect('servicosExecutados.db')
            cursor2 = conn2.cursor()
            select_query = 'SELECT Oid FROM ResultadoConsulta WHERE Oid = ?'
            oid = str(row[0])
            cursor2.execute(select_query, (oid,))
            existing_oid = cursor2.fetchone()
            if existing_oid is None:
                # O Oid não existe, então procedemos com a inserção
                insert_query = '''
                    INSERT INTO ResultadoConsulta (
                        Oid,
                        NumeroSolicitacao,
                        OrcamentoCRM,
                        Triagem,
                        IdentificacaoProtocoloAmostra,
                        NomeEnsaio,
                        NomeCliente,
                        TagTrafo,
                        NumeroSerieTrafo,
                        DataInicioServicoExecutavel,
                        DataFinalServicoExecutavel,
                        DataInicioProgramacao,
                        DataFinalProgramacao,
                        Situacao,
                        Parecer,
                        DataInformadaProtocolo,
                        DataAprovadoProtocolo,
                        Laboratorio,
                        Matriz,
                        DataChegadaAmostra,
                        DataProgFimCalculada
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                '''

                cursor2.execute(insert_query, (
                    oid,
                    row[1],
                    row[2],
                    row[3],
                    row[4],
                    row[5],
                    row[6],
                    row[7],
                    row[8],
                    row[9],
                    row[10],
                    row[11],
                    row[12],
                    row[13],
                    row[14],
                    row[15],
                    row[16],
                    row[17],
                    row[18],
                    row[19],
                    row[20],
                ))
            conn2.commit()
            conn2.close()

    except Exception as e:
        # print(f"Erro ao conectar: {str(e)}")
        conn.close()
        conn2.close()
        sys.exit(0)

    finally:
        # Certifique-se de fechar a conexão quando terminar
        conn.close()
        conn2.close()


def inserirCard():
    conn = sqlite3.connect('servicosExecutados.db')
    cursor = conn.cursor()
    cursor.execute('''
        SELECT 
            Oid,
            NumeroSolicitacao,
            OrcamentoCRM,
            Triagem,
            IdentificacaoProtocoloAmostra,
            NomeEnsaio,
            NomeCliente,
            TagTrafo,
            NumeroSerieTrafo,
            DataInicioServicoExecutavel,
            DataFinalServicoExecutavel,
            DataInicioProgramacao,
            DataFinalProgramacao,
            Situacao,
            Parecer,
            DataInformadaProtocolo,
            DataAprovadoProtocolo,
            Laboratorio,
            Matriz,
            DataChegadaAmostra,
            DataProgFimCalculada,
            idCard
        FROM ResultadoConsulta
        ORDER BY Triagem
    ''')
    linhas = cursor.fetchall()
    Triagem = ""
    for linha in linhas:
        if linha[21] == None:
            if Triagem != linha[3] and verificaSeExiste(linha[3]):
                Triagem = linha[3]
                updateCard(linha, linhas)

def verificaSeExiste(Triagem):
    conn = sqlite3.connect('servicosExecutados.db')
    cursor = conn.cursor()
    cursor.execute(f'''
            SELECT 
                Oid,
                NumeroSolicitacao,
                OrcamentoCRM,
                Triagem,
                IdentificacaoProtocoloAmostra,
                NomeEnsaio,
                NomeCliente,
                TagTrafo,
                NumeroSerieTrafo,
                DataInicioServicoExecutavel,
                DataFinalServicoExecutavel,
                DataInicioProgramacao,
                DataFinalProgramacao,
                Situacao,
                Parecer,
                DataInformadaProtocolo,
                DataAprovadoProtocolo,
                Laboratorio,
                Matriz,
                DataChegadaAmostra,
                DataProgFimCalculada,
                idCard
            FROM ResultadoConsulta
            WHERE Triagem = '{Triagem}'
            ORDER BY Triagem
        ''')
    linhas = cursor.fetchall()
    if linhas == None:
        return True
    else:
        AtualizarCard(linhas[0][21], linhas)
        return False

    conn.close()


def AtualizarCard(id, linhas):
    url = f"https://api.trello.com/1/cards/{id}"
    params = {
        "key": key,
        "token": token,
        "desc": returnStringDescricao(linhas[0], linhas)
    }
    response = requests.put(url, params=params)
    if response.status_code == 200:
        conn = sqlite3.connect('servicosExecutados.db')
        cursor = conn.cursor()
        card_id = response.json()["id"]
        for linha in linhas:
            update_query = f"""
                UPDATE ResultadoConsulta
                SET idCard = '{card_id}'
                WHERE Oid = '{linha[0]}'
            """
            cursor.execute(update_query)
        conn.commit()
        conn.close()
        # print(f"Falha ao atualizar o card. Código de status: {response.status_code}")
        # print(response.text)



def criarCard(linha, linhas):
    url_cards = "https://api.trello.com/1/cards"
    list = returListID(linha)
    if list != "":
        params = {
            "key": key,
            "token": token,
            "idList": list,
            "name": f"{linha[3]}  -  {linha[2]}",
            "desc": returnStringDescricao(linha, linhas),
            "idLabels": ",".join(returLabelID(linha)),  # Concatena os IDs das etiquetas separados por vírgula
            # "due": due_date,
            # "start": start_date,
            "pos": "top"
        }

        response = requests.post(url_cards, params=params)
        card_id = response.json()["id"]
        # if response.status_code != 200:
        #     print("Erro ao criar o card. Código de status:", response.status_code)
        #     print("Resposta:", response.text)
        return card_id

def returnStringDescricao(linha, linhas):
    Protocolos = ""
    for row in linhas:
        if row[3] == linha[3]:
            Protocolos = Protocolos+f'\n\n        {row[4]}\n       {row[5]}\n      {row[18]}'

    String = str(f"""
    {str(linha[1])}
    {str(linha[2])}
    {str(linha[3])}
    {str(linha[6])}
    {Protocolos}

            """)
    return String
def returLabelID(line):
    DataInicioStr = str(line[11]) if line[11] is not None else str(line[19])
    DataFinalStr = str(line[12]) if line[12] is not None else str(line[20])

    if DataInicioStr is not None and DataFinalStr is not None and DataInicioStr != 'None' and DataFinalStr != 'None':
        DataInicio = datetime.strptime(DataInicioStr, returFormatter(DataInicioStr))
        DataFinal = datetime.strptime(DataFinalStr, returFormatter(DataFinalStr))

        Urgencia = ""
        if DataInicioStr is not None and DataFinalStr is not None:
            Urgencia = (DataFinal - DataInicio).days


        if Urgencia >= 20:
            return  ['656f4f69c35d9b9c6d5a6134']
        elif Urgencia <= 20 and Urgencia >= 10:
            return  ['656f4f69c35d9b9c6d5a613a']
        elif Urgencia <= 10 and Urgencia >= 5:
            return  ['656f4f69c35d9b9c6d5a6139']
        elif Urgencia <= 5:
            return  ['656f4f69c35d9b9c6d5a6149']
    else:
        return ""


def returListID(line):
    DataFinalStr = str(line[12]) if line[12] is not None else str(line[20])

    if DataFinalStr is not None and DataFinalStr != 'None':
        DataFinal = datetime.strptime(DataFinalStr, returFormatter(DataFinalStr))

        PrazoRestante = ""
        if DataFinalStr is not None:
            PrazoRestante = (DataFinal - datetime.now()).days

        if PrazoRestante >= 20:
            return list20D
        elif PrazoRestante <= 20 and PrazoRestante >= 10:
            return list10D
        elif PrazoRestante <= 10 and PrazoRestante >= 5:
            return list05D
        elif PrazoRestante <= 5 and PrazoRestante >= 1:
            return list24H
        elif PrazoRestante <= 1 and PrazoRestante > 0:
            return "6570a0f52cc5822b1f325fc2"
        elif PrazoRestante <= 0:
            return listAtrasados
    else:
        return ""


def returFormatter(formatterProcurado):
    if formatterProcurado is not None:
        if len(formatterProcurado) == 26:
            formato_str = '%Y-%m-%d %H:%M:%S.%f'
            return formato_str
        elif len(formatterProcurado) == 19:
            formato_str = '%Y-%m-%d %H:%M:%S'
            return formato_str

def createDbLocal ():
    # Conectar ao banco de dados (isso criará um arquivo se não existir)
    conn = sqlite3.connect('servicosExecutados.db')

    # Criar um objeto cursor para executar comandos SQL
    cursor = conn.cursor()

    # Criar uma tabela de exemplo
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ResultadoConsulta (
            Oid TEXT,
            NumeroSolicitacao TEXT,
            OrcamentoCRM TEXT,
            Triagem TEXT,
            IdentificacaoProtocoloAmostra TEXT,
            NomeEnsaio TEXT,
            NomeCliente TEXT,
            TagTrafo TEXT,
            NumeroSerieTrafo TEXT,
            DataInicioServicoExecutavel TEXT,
            DataFinalServicoExecutavel TEXT,
            DataInicioProgramacao TEXT,
            DataFinalProgramacao TEXT,
            Situacao INTEGER,
            Parecer TEXT,
            DataInformadaProtocolo TEXT,
            DataAprovadoProtocolo TEXT,
            Laboratorio TEXT,
            Matriz TEXT,
            DataChegadaAmostra TEXT,
            DataProgFimCalculada TEXT,
            idCard TEXT
        )
    ''')

    # Commitar as alterações e fechar a conexão
    conn.commit()
    conn.close()

def updateCard(linha, linhas):
    conn = sqlite3.connect('servicosExecutados.db')
    cursor = conn.cursor()
    idDoCard = criarCard(linha, linhas)
    Triagem = linha[3]
    for linha_ne in linhas:
        if linha_ne[3] == Triagem:
            update_query = f"""
                UPDATE ResultadoConsulta
                SET idCard = '{idDoCard}'
                WHERE Oid = '{linha_ne[0]}'
            """
            cursor.execute(update_query)
    conn.commit()
    conn.close()

def connect_to_database(server, database, username, password):
    connection_string = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
    return pyodbc.connect(connection_string)
def controle_cards():
    server = '#####'
    database = '#####'
    username = '######'
    password = '######'

    try:
        with sqlite3.connect('servicosExecutados.db') as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT 
                    Oid, NumeroSolicitacao, OrcamentoCRM, Triagem, IdentificacaoProtocoloAmostra,
                    NomeEnsaio, NomeCliente, TagTrafo, NumeroSerieTrafo, DataInicioServicoExecutavel,
                    DataFinalServicoExecutavel, DataInicioProgramacao, DataFinalProgramacao, Situacao,
                    Parecer, DataInformadaProtocolo, DataAprovadoProtocolo, Laboratorio, Matriz,
                    DataChegadaAmostra, DataProgFimCalculada, idCard
                FROM ResultadoConsulta
                ORDER BY Triagem
            ''')
            linhas = cursor.fetchall()
            ultimo_modificado = ""

            for linha in linhas:
                if linha[3] != ultimo_modificado:
                    with connect_to_database(server, database, username, password) as conn2:
                        cursor2 = conn2.cursor()
                        cursor2.execute(f"""
                        SELECT
                        ServicoExecutado.Oid,
                        solicitacaoServico.NumeroSolicitacao,
                        SolicitacaoServico.OrcamentoCRM,
                        Protocolo.Identificacao as Triagem,
                        ProtocoloAmostra.Identificacao,
                        obj2.Identificacao as NomeEnsaio,
                        Cliente.Nome as NomeCliente,
                        ObjetoAmostravelTransformador.Tag as TagTrafo,
                        ObjetoAmostravel.Identificacao as NumeroSerieTrafo,
                        ServicoExecutado.DataInicialAnalise as DataInicioServicoExecutavel,
                        ServicoExecutado.DataFinalAnalise as DataFinalServicoExecutavel,
                        ProgramacaoServicosAgenda.StartOn as DataInicioProgramacao,
                        ProgramacaoServicosAgenda.EndOn as DataFinalProgramacao,
                        ServicoExecutado.Situacao,
                        ServicoExecutado.Parecer,
                        Protocolo.Data as DataInformadaProtocolo,
                        Protocolo.ApprovedOn as DataAprovadoProtocolo,
                        LAB.Nome as Laboratorio,
                        MatrizOuTipo.Nome as Matriz,
                        ProtocoloAmostra.DataChegadaAmostra,
                        ObjetoSolicitado.DataProgFimCalculada
                        FROM
                        ServicoExecutado

                        INNER JOIN ExecucaoServico ON ServicoExecutado.ExecucaoServico = ExecucaoServico.Oid
                        INNER JOIN AmostraObjeto ON ExecucaoServico.Amostra = AmostraObjeto.Oid
                        INNER JOIN ProtocoloAmostra ON ProtocoloAmostra.Oid=AmostraObjeto.ProtocoloAmostra
                        INNER JOIN Protocolo ON ProtocoloAmostra.Protocolo=Protocolo.oid
                        LEFT JOIN ObjetoAmostravelTransformador ON ProtocoloAmostra.ObjetoTransformador=ObjetoAmostravelTransformador.Oid
                        LEFT JOIN ObjetoAmostravel ON ObjetoAmostravelTransformador.Oid = ObjetoAmostravel.Oid
                        INNER JOIN ObjetoSolicitado ON AmostraObjeto.ObjetoSolicitado=ObjetoSolicitado.oid

                        INNER JOIN MatrizOuTipo ON ProtocoloAmostra.MatrizOuTipo=MatrizOuTipo.Oid
                        INNER JOIN ObjetoAmostravel obj2 ON AmostraObjeto.Objeto = obj2.Oid
                        INNER JOIN SolicitacaoServico ON Protocolo.SolicitacaEnsaio=SolicitacaoServico.Oid
                        INNER JOIN Cliente ON SolicitacaoServico.AreaSolicitante=Cliente.Oid
                        LEFT JOIN ProgramacaoServicosAgenda ON ProgramacaoServicosAgenda.Oid=ServicoExecutado.Programacao
                        INNER JOIN Cliente lab ON ExecucaoServico.ObjectOwner=LAB.Oid

                        WHERE ExecucaoServico.GCRecord IS NULL AND ServicoExecutado.GCRecord is null
                              and ProtocoloAmostra.Identificacao is not null
                              and Lab.nome='Lab. de Óleos' AND
                              Protocolo.CreatedOn >= '2023-01-01' AND
                              Protocolo.Identificacao = '{linha[3]}'
                            """)
                        rows = cursor2.fetchall()
                        moveCARD(linha[21], returListID(linha))

                        if verificarTodoProtocolo(rows):
                            card_mapping = {
                                1: "656f4fc1efa73633288defa4",
                                2: "656f4fc7bede59a31f989fac",
                                3: "656f4fca821a3a9d46d829b6",
                                4: "6570a2ffbb6c62d524159741",
                                5: "6570a3078e4d53d23926f5f8"
                            }

                            target_list_id = card_mapping.get(rows[0][13])
                            if target_list_id:
                                moveCARD(linha[21], target_list_id)
                                ultimo_modificado = linha[3]

    except Exception as e:
        # Log the error or handle it appropriately
        print(f"Erro: {str(e)}")
        sys.exit()


def moveCARD(card_id, new_list_id):
    url = f"https://api.trello.com/1/cards/{card_id}"
    params = {
        "key": key,
        "token": token,
        "idList": new_list_id
    }

    response = requests.put(url, params=params)

    # if response.status_code != 200:
    #     print("Erro ao mover o card. Código de status:", response.status_code)
    #     print("Resposta:", response.text)


def verificarTodoProtocolo(rows):
    return all(linha[13] == rows[0][13] for linha in rows)

conectToSQL()
inserirCard()
controle_cards()

# situacao incompleta 0
# situacao completa 1
# situacao validado 2
# situacao finalizado 3
# situacao cancelado 4
# situacao serviço em cancelamento 4