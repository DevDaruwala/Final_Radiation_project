import os

input_file = "/media/dev/Expansion/Big_data/raw_data.csv"
output_file = "/media/dev/Expansion/Big_data/Small_file.csv"

# Extarcting 1GB = 1,000,000,000 bytes
target_size_bytes = 1_000_000_000

def extract_first_1gb(input_file, output_file, max_bytes):
    total_bytes = 0
    # we will start to read and write in new file
    with open(input_file, 'r', encoding='utf-8') as fin, open(output_file, 'w', encoding='utf-8') as fout:
        # Write header
        header = fin.readline()
        fout.write(header)
        total_bytes += len(header.encode('utf-8'))

        # loop till 1gb
        for line in fin:
            encoded = line.encode('utf-8')
            if total_bytes + len(encoded) > max_bytes:
                break
            fout.write(line)
            total_bytes += len(encoded)

    print(f"Extraction complete. Total written: {total_bytes / (1024 ** 2):.2f} MB")

extract_first_1gb(input_file, output_file, target_size_bytes)