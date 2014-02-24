from CRABClient.Commands import remote_copy

class remoteCopy(remote_copy):

    def __init__(self, dictresult, destination, logger):
        self.options = {}
        self.options.dictresult = dictresult
        self.options.destination = destination
        self.logger = logger
