import argparse
import subprocess


def archive(url, headers):
    


def main(repo):
    


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-r', '--repo', type=str, required=True,
                        'The mercurial repository to archive.')
    args = parser.parse_args()
    main(args.repo)

