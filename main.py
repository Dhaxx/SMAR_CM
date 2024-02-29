from modulos.compras import base
from modulos.compras import solicitacoes
from modulos.compras import licitacao
from modulos.compras import cotacoes


def main():
    # base.cadunimedida()
    # base.grupo_e_subgrupo()
    # base.cadest()
    # base.almoxarifado()
    # base.centro_custo()
    # # solicitacoes.cadastro()
    cotacoes.cadastro()
    cotacoes.fornecedores()
    cotacoes.valores()
    # cadastra_fornecedor()
    # licitacao.cadlic()
    licitacao.prolic()
    

if __name__ == '__main__':
    main()
