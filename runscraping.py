import subprocess


def main():
    try:
        out = subprocess.check_output("scrapy crawl instagram -o test.csv", shell=True, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        raise RuntimeError("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))

if __name__ == "__main__":



    print(out)
