from modulos.compras import base
from modulos.compras import solicitacoes
from modulos.compras import licitacao
from modulos.compras import cotacoes
from modulos import tools


def main():
    # base.cadunimedida()
    # base.grupo_e_subgrupo()
    # base.cadest()
    # base.almoxarifado()
    # base.centro_custo()
    # solicitacoes.cadastro()
    # cotacoes.cadastro()
    # tools.ajustar_ccusto_cotacao()
    # cotacoes.fornecedores()
    # cotacoes.valores()
    # tools.cadastra_fornecedor()
    # licitacao.cadlic()
    # licitacao.cadprolic()
    # licitacao.prolic_prolics()
    # licitacao.cadpro_proposta() # Esperar a cotação
    # licitacao.cadpro_lance() # Esperar a cotação
    # licitacao.cadpro_final() # Esperar a cotação
    # licitacao.cadpro_status() # Esperar a cotação
    # licitacao.cadpro() # Esperar a cotação
    # licitacao.regpreco() # Esperar a cotação
    licitacao.aditamento()

if __name__ == '__main__':
    main()
