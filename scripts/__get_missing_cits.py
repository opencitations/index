import argparse
import csv

def remove_duplicates(input_file, output_file):
    try:
        # Create a set to store unique rows
        unique_rows = set()

        # Read the input CSV file and collect unique rows
        with open(input_file, 'r') as infile:
            reader = csv.reader(infile)
            for row in reader:
                unique_rows.add(tuple(row))

        # Write the unique rows to the output CSV file
        with open(output_file, 'w', newline='') as outfile:
            writer = csv.writer(outfile)
            writer.writerows(unique_rows)

        print(f"Duplicates removed. Result saved to {output_file}")

    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Remove duplicate rows from a CSV file.")
    parser.add_argument("input_file", help="Input CSV file")
    parser.add_argument("output_file", help="Output CSV file")

    args = parser.parse_args()

    remove_duplicates(args.input_file, args.output_file)
