# I am Satoshi

from conexao import *
from ..tools import *
from tqdm import tqdm

PRODUTOS = produtos()


def cadastro():
    print("Inserindo Cadastro de Cotações...")

    global PRODUTOS

    if len(PRODUTOS) == 0:
        PRODUTOS = produtos()
