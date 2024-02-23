from conexao import *
from ..tools import *
from tqdm import tqdm

PRODUTOS = cur_fdb.execute("select cadpro, codreduz from cadest").fetchall()

def solicitacoes():
    cur_fdb.execute("icadorc")
    cur_fdb.execute("cadorc")

