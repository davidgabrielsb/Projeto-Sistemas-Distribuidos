import zmq

with zmq.Context() as contexto:
    with contexto.socket(zmq.ROUTER) as clientes, contexto.socket(zmq.DEALER) as servidores:
        clientes.bind("tcp://*:5555")
        servidores.bind("tcp://*:5556")

        poller = zmq.Poller()
        poller.register(clientes, zmq.POLLIN)
        poller.register(servidores, zmq.POLLIN)
        print("Broker pronto: clientes em 5555, servidores em 5556", flush=True)

        while True:
            eventos = dict(poller.poll())
            if clientes in eventos:
                servidores.send_multipart(clientes.recv_multipart())
            if servidores in eventos:
                clientes.send_multipart(servidores.recv_multipart())
