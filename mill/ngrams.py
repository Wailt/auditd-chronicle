from enumerator import enumerator


class Ngrams:
    def __init__(self, sessions, n=2, level = 0):
        self.ngrams = dict()
        for s in sessions:
            for h in range(1, n + 1):
                for i in range(len(sessions[s]) - h + 1):
                    self.ngrams.setdefault(tuple(sessions[s][i:i + h]), 0)
                    self.ngrams[tuple(sessions[s][i:i + h])] += 1

        self.ngrams = {i: self.ngrams[i] for i in self.ngrams if self.ngrams[i] >= level}

        self.ngram_iterator = enumerator()
        for i in self.ngrams:
            self.ngram_iterator.get_number(i)

    def push_stats_to_es(self):
        pass

    def get_stats_from_es(self):
        pass