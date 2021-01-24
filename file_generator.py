def generate_big_random_bin_file(filename, size):
    """
    generate big binary file with the specified size in bytes
    :param filename: the filename
    :param size: the size in bytes
    :return:void
    """
    import os 
    with open('%s'%filename, 'wb') as fout:
        fout.write(os.urandom(size)) #1

def generate_big_random_letters(filename, size):
    """
    generate big random letters/alphabets to a file
    :param filename: the filename
    :param size: the size in bytes
    :return: void
    """
    import random
    import string

    chars = ''.join([random.choice(string.letters) for i in range(size)]) #1

    with open(filename, 'w') as f:
        f.write(chars)
    pass

def generate_big_sparse_file(filename, size):
    f = open(filename, "wb")
    f.seek(size - 1)
    f.write("\1")
    f.close()
    pass

def generate_big_random_sentences(filename, linecount):
    import random
    nouns = ("puppy", "car", "rabbit", "girl", "monkey")
    verbs = ("runs", "hits", "jumps", "drives", "barfs")
    adv = ("crazily.", "dutifully.", "foolishly.", "merrily.", "occasionally.")
    adj = ("adorable", "clueless", "dirty", "odd", "stupid")

    all = [nouns, verbs, adj, adv]

    with open(filename,'w') as f:
        for i in range(linecount):
            f.writelines([' '.join([random.choice(i) for i in all]),'\n'])
    pass