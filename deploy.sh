#!/bin/bash

start_time=$(date +%s)
log_file="execution_log.txt"

# Fonction pour calculer la durée et afficher le temps écoulé
function log_time {
    local step_start_time=$1
    local step_name=$2
    local step_end_time=$(date +%s)
    local step_duration=$((step_end_time - step_start_time))
    echo "$step_name : $step_duration secondes" >> $log_file
}

# Définir les chemins et les fichiers
login="dejurquet-24"
localFolder="./dossierAdeployer"
remoteFolder="/home/users/dejurquet-24/dejurquet"
nameOfTheScript="script.py"
sourceFolder="/cal/commoncrawl" # Emplacement des fichiers source
blockSize=64M # Taille des blocs découpés
num_machines_to_use=50 # Définir le nombre maximal de machines à utiliser

# Initialiser le log
echo "Déploiement en cours avec $num_machines_to_use machines souhaitées" > $log_file

# Étape : Vérification des connexions SSH
echo "Vérification des connexions SSH..." >> $log_file
valid_machines=()
for ip in $(cat machines.txt); do
    if ssh -o BatchMode=yes -o ConnectTimeout=5 "$login@$ip" "echo 'Connexion réussie' > /dev/null 2>&1"; then
        valid_machines+=("$ip")
        echo "Connexion réussie : $ip" >> $log_file
        if [ "${#valid_machines[@]}" -ge "$num_machines_to_use" ]; then
            break
        fi
    else
        echo "Connexion échouée : $ip" >> $log_file
    fi
done

# Calculer le nombre réel de machines valides
num_machines=${#valid_machines[@]}

# Vérification si suffisamment de machines sont disponibles
if [ "$num_machines" -lt "$num_machines_to_use" ]; then
    echo "Nombre de machines valides disponible : $num_machines (moins que $num_machines_to_use souhaitées)." >> $log_file
fi
if [ "$num_machines" -eq 0 ]; then
    echo "Aucune machine valide disponible. Arrêt du script." >> $log_file
    exit 1
fi

echo "Machines sélectionnées ($num_machines disponibles) : ${valid_machines[*]}" >> $log_file

# Étape 1 : Téléchargement des fichiers
mkdir -p "$localFolder"
ssh "$login@${valid_machines[0]}" "ls $sourceFolder" > "$localFolder/source_files.txt"

# Lire les x premiers fichiers disponibles
filesToDownload=$(head -n 5 "$localFolder/source_files.txt")
total_size=0

# Télécharger les fichiers
step_start_time=$(date +%s)
for file in $filesToDownload; do
    echo "Tentative de téléchargement du fichier : $file" >> $log_file
    if scp "$login@${valid_machines[0]}:$sourceFolder/$file" "$localFolder/"; then
        echo "Téléchargement réussi : $file" >> $log_file
        file_size=$(stat -c%s "$localFolder/$file")
        total_size=$((total_size + file_size))
    else
        echo "Erreur : Impossible de télécharger $file. Passer au fichier suivant." >> $log_file
        continue
    fi
done
log_time $step_start_time "Téléchargement des fichiers"

# Ajouter le poids total des fichiers téléchargés au log
echo "Poids total des fichiers traités : $((total_size / 1024 / 1024)) Mo" >> $log_file

# Étape 2 : Découpage des fichiers
step_start_time=$(date +%s)
part_counter=1
for file in "$localFolder"/*; do
    if [[ -f "$file" ]]; then
        echo "Découpage du fichier : $file" >> $log_file
        split -b $blockSize "$file" "$localFolder/tmp_part_"
        for block in "$localFolder"/tmp_part_*; do
            mv "$block" "$localFolder/part${part_counter}"
            part_counter=$((part_counter + 1))
        done
    else
        echo "Fichier $file introuvable ou inaccessible pour le découpage." >> $log_file
    fi
done
log_time $step_start_time "Découpage des fichiers"

# Étape 3 : Répartition des blocs
step_start_time=$(date +%s)
for ip in "${valid_machines[@]}"; do
    ssh "$login@$ip" "rm -rf $remoteFolder && mkdir -p $remoteFolder"
done
i=1
for block in "$localFolder"/part*; do
    targetMachine=${valid_machines[$(( (i - 1) % num_machines ))]} # Utilise num_machines
    scp "$block" "$login@$targetMachine:$remoteFolder/"
    i=$((i + 1))
done
log_time $step_start_time "Répartition des blocs"

# PHASE DE MAPPING (Exécution parallèle)
step_start_time=$(date +%s)
i=1
for ip in "${valid_machines[@]}"; do
    (
        echo "Démarrage du mapping sur la machine $ip"
        scp "$localFolder/$nameOfTheScript" "$login@$ip:$remoteFolder/"
        attempt=1
        max_attempts=5
        success=0
        while [ $attempt -le $max_attempts ]; do
            blocks=$(ssh "$login@$ip" "ls $remoteFolder/part* 2>/dev/null")
            if [ -z "$blocks" ]; then
                echo "Aucun bloc trouvé sur $ip après $attempt tentative(s)"
                break
            fi
            for block in $blocks; do
                ssh "$login@$ip" "cd $remoteFolder && python3 \"$nameOfTheScript\" mapping \"$i\" \"$block\" \"$remoteFolder/mapping_${i}_${block##*/}.txt\""
                if [ $? -ne 0 ]; then
                    echo "Échec du mapping pour $block sur $ip"
                    success=0
                    break
                else
                    success=1
                    echo "Mapping terminé pour $block sur $ip"
                fi
            done
            if [ $success -eq 1 ]; then
                break
            fi
            attempt=$((attempt + 1))
            sleep $((attempt * 2))
        done
        if [ $success -ne 1 ]; then
            echo "Échec complet du mapping sur la machine $ip après $max_attempts tentatives"
        else
            echo "Mapping réussi sur la machine $ip"
        fi
    ) &
    i=$((i + 1))
done
wait
log_time $step_start_time "Phase de mapping"

# PHASE DE SHUFFLE (Exécution parallèle avec gestion des erreurs)
step_start_time=$(date +%s)
i=1
for ip in "${valid_machines[@]}"; do
    {
        attempt=1
        max_attempts=5
        while [ $attempt -le $max_attempts ]; do
            ssh "$login@$ip" "cd $remoteFolder && python3 $nameOfTheScript shuffle $i $remoteFolder $remoteFolder $num_machines"
            if [ $? -eq 0 ]; then
                echo "Phase de shuffle réussie sur la machine $ip pour part $i"
                break
            fi
            echo "Erreur dans la phase de shuffle sur la machine $ip pour part $i, tentative $attempt/$max_attempts"
            attempt=$((attempt + 1))
            sleep $((attempt * 2))
        done
        if [ $attempt -gt $max_attempts ]; then
            echo "Échec complet de la phase de shuffle sur la machine $ip pour part $i"
        fi
    } &
    i=$((i + 1))
done
wait
log_time $step_start_time "Phase de shuffle"

# PHASE DE REDUCE (Exécution parallèle avec gestion des erreurs)
step_start_time=$(date +%s)
i=1
for ip in "${valid_machines[@]}"; do
    {
        ssh "$login@$ip" "cd $remoteFolder && cat shuffle_${i}_from_machine_*.txt > reduce_input_${i}.txt"
        attempt=1
        max_attempts=5
        while [ $attempt -le $max_attempts ]; do
            ssh "$login@$ip" "cd $remoteFolder && python3 $nameOfTheScript reduce $i $remoteFolder/reduce_input_${i}.txt $remoteFolder/reduce_machine${i}.txt"
            if [ $? -eq 0 ]; then
                echo "Phase de réduction réussie sur la machine $ip pour part $i"
                break
            fi
            echo "Erreur dans la phase de réduction sur la machine $ip pour part $i, tentative $attempt/$max_attempts"
            attempt=$((attempt + 1))
            sleep $((attempt * 2))
        done
        if [ $attempt -gt $max_attempts ]; then
            echo "Échec complet de la phase de réduction sur la machine $ip pour part $i"
        fi
    } &
    i=$((i + 1))
done
wait
log_time $step_start_time "Phase de réduction"

# PHASE D'AGRÉGATION
step_start_time=$(date +%s)
result_file="$localFolder/resultat_final.txt"

# Supprimer un ancien résultat, s'il existe
rm -f "$result_file"
touch "$result_file"

echo "Récupération des fichiers reduce depuis les machines distantes..." >> $log_file
i=1
for ip in "${valid_machines[@]}"; do
    attempt=1
    max_attempts=5
    while [ $attempt -le $max_attempts ]; do
        scp "$login@$ip:$remoteFolder/reduce_machine${i}.txt" "$localFolder/"
        if [ $? -eq 0 ]; then
            echo "Fichier reduce_machine${i}.txt récupéré depuis la machine $ip"
            break
        fi
        echo "Échec de récupération de reduce_machine${i}.txt depuis la machine $ip, tentative $attempt/$max_attempts"
        attempt=$((attempt + 1))
        sleep $((attempt * 2))
    done
    if [ $attempt -gt $max_attempts ]; then
        echo "Impossible de récupérer reduce_machine${i}.txt depuis la machine $ip après $max_attempts tentatives"
    fi
    i=$((i + 1))
done

# Combinaison des fichiers reduce en un seul fichier local
echo "Agrégation des fichiers reduce..." >> $log_file
cat "$localFolder"/reduce_machine*.txt > "$result_file"

# Nettoyer les fichiers intermédiaires
rm -f "$localFolder"/reduce_machine*.txt

log_time $step_start_time "Phase d'agrégation"
echo "Résultat final disponible dans : $result_file" >> $log_file
