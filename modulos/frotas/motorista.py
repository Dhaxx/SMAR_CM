from conexao import *
from ..tools import *
from tqdm import tqdm

def cadastro():
    cur_fdb.execute('delete from motor')
    consulta = fetchallmap("""
                           select
                                    pk,
                                    nome,
                                    num_cnh,
                                    categoria_cnh,
                                    venc_cnh,
                                    case
                                        when situacao = 'Inativo' then 'S'
                                        else Null
                                    end bloqmotor,
                                        cpf,
                                        data_nasc,
                                        pontos
                                from
                                        mascara_motorista mm""")
    
    insert = cur_fdb.prep("""insert into motor (cod, nome, cnh, dtvenccnh, categcnh, cpf, dtnascimento, pontoscnh, bloqmotor) values (?,?,?,?,?,?,?,?,?)""")

    for row in tqdm(consulta, desc='FROTAS - Cadastrando Motoristas'):
        cod = row['pk']
        nome = row['nome']
        cnh = row['num_cnh']
        dtvenccnh = row['venc_cnh']
        categcnh = row['categoria_cnh']
        cpf = row['cpf']
        dtnascimento = row['data_nasc']
        pontos_cnh = row['pontos']
        bloqmotor = row['bloqmotor']
        cur_fdb.execute(insert,(cod,nome,cnh,dtvenccnh,categcnh,cpf,dtnascimento,pontos_cnh,bloqmotor))
    commit()