import sys
import re
import os
import hashlib

def map_phase(input_file, output_file):
    word_count = []
    try:
        with open(input_file, 'rb') as file:  # Lire en mode binaire pour gérer les erreurs d'encodage
            for line in file:
                try:
                    decoded_line = line.decode('utf-8')  # Essayer de décoder en UTF-8
                except UnicodeDecodeError:
                    decoded_line = line.decode('utf-8', errors='ignore')  # Ignorer les caractères invalides
                
                words = re.findall(r'\b\w+\b', decoded_line.lower())
                for word in words:
                    word_count.append(f"{word} 1")
        
        output_dir = os.path.dirname(output_file)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        with open(output_file, 'w') as file:
            for entry in word_count:
                file.write(entry + '\n')
        print(f"Fichier de mapping créé : {output_file}")

    except Exception as e:
        print(f"Erreur lors du mapping : {e}")

def shuffle_phase(machine_id, input_folder, output_folder, num_machines):
    word_map = {}

    for filename in os.listdir(input_folder):
        if filename.startswith("mapping_"):
            try:
                with open(os.path.join(input_folder, filename), 'r') as file:
                    for line in file:
                        try:
                            word, count = line.split()
                            if word in word_map:
                                word_map[word] += int(count)
                            else:
                                word_map[word] = int(count)
                        except ValueError:
                            print(f"Ligne mal formée ignorée dans {filename}: {line.strip()}")
            except Exception as e:
                print(f"Erreur lors de la lecture du fichier {filename}: {e}")

    partitions = {i: [] for i in range(num_machines)}

    for word, count in word_map.items():
        try:
            partition_key = int(hashlib.md5(word.encode()).hexdigest(), 16) % num_machines
            partitions[partition_key].append((word, count))
        except Exception as e:
            print(f"Erreur lors du partitionnement du mot {word}: {e}")

    for target_machine_id in range(num_machines):
        output_file = os.path.join(output_folder, f"shuffle_{target_machine_id + 1}_from_machine_{machine_id}.txt")
        try:
            with open(output_file, 'w') as file:
                for word, count in partitions[target_machine_id]:
                    file.write(f"{word} {count}\n")
            print(f"Fichier de shuffle créé : {output_file}")
        except Exception as e:
            print(f"Erreur lors de la création du fichier de shuffle {output_file}: {e}")

def reduce_phase(input_file, output_file):
    word_count = {}

    try:
        with open(input_file, 'r') as file:
            for line in file:
                try:
                    word, count = line.split()
                    if word in word_count:
                        word_count[word] += int(count)
                    else:
                        word_count[word] = int(count)
                except ValueError:
                    print(f"Ligne mal formée ignorée dans {input_file}: {line.strip()}")

        output_dir = os.path.dirname(output_file)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        sorted_words = sorted(word_count.items(), key=lambda x: (-x[1], x[0]))

        with open(output_file, 'w') as file:
            for word, count in sorted_words:
                file.write(f"{word} {count}\n")
        print(f"Fichier de réduction créé : {output_file}")
    except Exception as e:
        print(f"Erreur lors de la réduction : {e}")

if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("Usage : python3 script.py <phase> <machine_id> <input_path> <output_path> [num_machines]")
        sys.exit(1)
    
    phase = sys.argv[1]
    machine_id = int(sys.argv[2])
    input_path = sys.argv[3]
    output_path = sys.argv[4]

    if phase == "mapping":
        print(f"Début Phase 1: Mapping sur la machine {machine_id}")
        map_phase(input_path, output_path)
        print(f"OK Phase 1: Mapping terminé sur la machine {machine_id}")
    
    elif phase == "shuffle":
        if len(sys.argv) != 6:
            print("Erreur : Nombre de machines (num_machines) requis pour la phase shuffle.")
            sys.exit(1)
        num_machines = int(sys.argv[5])
        print(f"Début Phase 2: Shuffling Machine {machine_id} avec {num_machines} machines")
        shuffle_phase(machine_id, input_path, output_path, num_machines)
        print(f"OK Phase 2: Shuffling Machine {machine_id}")
    
    elif phase == "reduce":
        print(f"Début Phase 3: Reducing Machine {machine_id}")
        reduce_phase(input_path, output_path)
        print(f"OK Phase 3: Reducing Machine {machine_id}")
    
    else:
        print(f"Phase non reconnue : {phase}")
        sys.exit(1)
