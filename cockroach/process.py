def main():
    f1 = '/Users/Administrator/Desktop/cs5424db/project_files/xact_files_A/'
    f2 = '/Users/Administrator/Desktop/cs5424db/my_xact/'
    # for i in range(0, 5):
    #     with open(f1 + str(i) + '.txt', 'r') as fr:
    #         lines = fr.readlines()
    #         with open(f2 + str(i) + '.txt', 'w+') as fw:
    #             for x in range(0, 100):
    #                 fw.write(lines[x])


    for i in range(1, 8):
        for j in range(0, 5):
            with open(f2 + str(j) + '.txt', 'r') as fr:
                lines = fr.readlines()
                with open(f2 + str(i * 5 + j) + '.txt', 'w+') as fw:
                    for x in range(0, len(lines)):
                        fw.write(lines[x])


if __name__ == "__main__":
    main()
