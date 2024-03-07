from conexao import *
from ..tools import *
from tqdm import tqdm

PRODUTOS = produtos()
LICITACAO = licitacoes()

def cabecalho():
    print('Inserindo Cabecalho de pedidos...')

    consulta = fetchallmap("""""")