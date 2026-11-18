import time

from consultar import consultar


def ler_eventos(nome):
    eventos = {}
    cursor = 0
    while True:
        resposta = consultar(f'tcp://{nome}:5570', 'eventos', {'apos': cursor})
        lote = resposta['dados']['eventos']
        for evento in lote:
            eventos[evento['id']] = (evento['tabela'], evento['dados'])
        if len(lote) < 100:
            return eventos
        cursor = lote[-1]['seq']


def main():
    nomes = [f'{linguagem}-server-{numero}' for linguagem in ['python', 'java', 'javascript'] for numero in [1, 2]]
    limite = time.monotonic() + 90
    while time.monotonic() < limite:
        copias = [ler_eventos(nome) for nome in nomes]
        if all(copia == copias[0] for copia in copias):
            print(f'OK: os seis servidores possuem os mesmos {len(copias[0])} eventos.', flush=True)
            return
        print('Aguardando a propagacao das replicas...', flush=True)
        time.sleep(2)
    raise SystemExit('As replicas ainda diferem. Consulte os logs dos servidores.')


if __name__ == '__main__':
    main()
