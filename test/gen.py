import os 
import string
import random


def randomString(stringLength=10):
    """Generate a random string of fixed length """
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(stringLength))

def makeTestFile(rows, cols):
    print("Generating " + str(rows) + " line file with " + str(cols) + " columns")
    dir_path = os.getcwd()
    os.makedirs(dir_path, exist_ok=True)
    idx = 0
    random.seed(1)
    f = open(os.path.join(dir_path+"/test", "test"+str(rows)+".sor"), "w")
    for i in range(rows):
        for j in range(cols):
            if j == 0 or j == 4 or j == 8:
                f.write("<" + str((i % 357) % 2) + ">")
            elif j == 1 or j == 5 or j == 9:
                f.write("<" + str(idx / 100) + ">")
            elif j == 2 or j == 6:
                f.write("<\"" + randomString(15) + "\">")
            elif j == 3 or j == 7:
                f.write("<" + str(idx) + ">")
            idx+=1
        if (i != rows - 1):
            f.write("\n")
    f.close()

makeTestFile(1000, 10)
#makeTestFile(10000, 10)
#makeTestFile(100000, 10)
#makeTestFile(1000000, 10)