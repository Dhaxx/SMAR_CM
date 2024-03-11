from conexao import *
from ..tools import *
from tqdm import tqdm

def modelo():
    cur_fdb.execute('delete from veiculo_tipo')

    consulta = fetchallmap("""
                           select
                                    DISTINCT(tipo_veiculo) descricao
                                from
                                    mascara_veiculo_modelo mvm
                                order by
                                    tipo_veiculo""")

    insert = cur_fdb.prep("""INSERT INTO VEICULO_TIPO (codigo_tip, descricao_tip) values (?,?)""")

    i = 0

    for row in tqdm(consulta, desc='FROTAS - Cadastrando Modelos'):
        i += 1
        cur_fdb.execute(insert,(i, row['descricao']))
    commit()

def marca():
    cur_fdb.execute('delete from veiculo_marca')

    consulta = fetchallmap("""select pk, descricao from veiculo_marca vm""")

    for row in consulta:
        cur_fdb.execute("""INSERT INTO VEICULO_MARCA (codigo_mar, codigo_tip_mar, descricao_mar) values (?,?,?)""", (row['pk'], row['pk'], row['descricao']))
    commit()

def cadastro():
    cur_fdb.execute("""delete from veiculo_historico""")
    cur_fdb.execute("""delete from veiculo""")
    cria_campo('alter table veiculo add codant_patrimonio varchar(12)')

    modelos = veiculo_tipo()
    marcas = veiculo_marca()

    insert = cur_fdb.prep("""
                            insert into veiculo (placa, modelo, combustivel, aquisicao, anomod, ano, renavam, chassi, sequencia, tanque, 
                                                 codigo_tipo_vei, kmatual, codant_patrimonio, codigo_marca_vei, cor, inativo) 
                             values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""")

    consulta = fetchallmap("""select
                                pk,
                                substring(replace(placa,'-',''),1,7) placa,
                                substring(descricao,1,45) descricao,
                                veiculo,
                                data_compra,
                                ano_fabric,
                                ano_modelo,
                                renavam,
                                chassi,
                                prefixo_veiculo,
                                uo,
                                nrPatrimonio,
                                tipoModelo,
                                medidor_atual,
                                cor,
                                marca,
                                SUBSTRING(combustivel,1,1) combustivel,
                                deslocamento,
                                CASE when situacao = 'Inativo' then 'S' else 'N' end inativo,
                                tanque
                            from
                                mascara_veiculo mv""")

    for row in tqdm(consulta, desc='FROTAS - Cadastrando Veiculos'):
        placa = row['placa']
        modelo = row['descricao']
        combustivel = row['combustivel']
        aquisicao = row['data_compra']
        ano = row['ano_fabric']
        anomod = row['ano_modelo']
        renavam = row['renavam']
        chassi = row['chassi']
        sequencia = row['pk']
        tanque = row['tanque']
        codigo_tipo_vei = modelos[row['tipoModelo']]
        kmatual = row['medidor_atual']
        inativo = row['inativo']
        codant_patrimonio = row['nrPatrimonio']
        codigo_marca_vei = marcas[row['marca']]
        cor = row['cor']
        
        cur_fdb.execute(insert,(placa, modelo, combustivel, aquisicao, anomod, ano, renavam, chassi, 
                                sequencia, tanque, codigo_tipo_vei, kmatual, codant_patrimonio, codigo_marca_vei, cor, inativo))
    commit()