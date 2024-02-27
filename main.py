from modulos.compras import base
from modulos.compras import solicitacoes
from modulos.compras import licitacao


def main():
    base.cadunimedida()
    base.grupo_e_subgrupo()
    base.cadest()
    base.almoxarifado()
    base.centro_custo()
    solicitacoes.solicitacoes()
    # licitacao.cadlic()


if __name__ == '__main__':
    main()
