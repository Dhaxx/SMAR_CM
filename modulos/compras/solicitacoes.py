from conexao import *
from ..tools import *
from tqdm import tqdm

PRODUTOS = cur_fdb.execute("select cadpro, codreduz from cadest").fetchall()

def solicitacoes():
    cur_fdb.execute("delete from icadorc")
    cur_fdb.execute("delete from cadorc")
    print("Inserindo Solicitações...")

    consulta = fetchallmap(f"""select
                                    right('00000' + cast(numreq as varchar),
                                    5)+ '/' + SUBSTRING(anoreq, 3, 5) numorc,
                                    right('00000' + cast(numreq as varchar),5) num,
                                    anoreq ano,
                                    datreq dtorc,
                                    motdev descr,
                                    'NORMAL' prioridade,
                                    CASE 
                                        when obs is not null then obs
                                        else motdev
                                    END obs,
                                    'AP' status,
                                    'S' liberado,
                                    idnivel5 codccusto,
                                    'L' liberado_tela,
                                    ROW_NUMBER() over (order by numreq) id_cadorc
                                from
                                    mat.mct63400
                                where
                                    anoreq in ({ANO}, {ANO - 1})""")
    
    insert = cur_fdb.prep("""insert
                                into
                                cadorc (id_cadorc,
                                num,
                                ano,
                                numorc,    
                                dtorc,
                                descr,  
                                prioridade,
                                obs,
                                status,
                                liberado,
                                codccusto,
                                liberado_tela,
                                empresa) values (?,?,?,?,?,?,?,?,?,?,?,?,?)""")
    
    for row in tqdm(consulta):
        id_cadorc = row['id_cadorc']
        num = row['num']
        ano = row['ano']
        dtorc = row['dtorc']
        descr =   row['descr'][:1024] if row['descr'] else row['descr']
        prioridade = row['prioridade']
        obs = row['obs']
        status = row['status']
        liberado = row['liberado']
        codccusto = row['codccusto']
        liberado_tela = row['liberado_tela']
        numorc = row['numorc']

        cur_fdb.execute(insert,(id_cadorc, num, ano, numorc, dtorc, descr, prioridade, obs, status, liberado, codccusto, liberado_tela, EMPRESA))
    commit()