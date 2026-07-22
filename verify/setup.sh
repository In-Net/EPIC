export proj_dir=$(pwd)

function compile() {
    if [[ $# -ne 1 ]]; then
        echo "Usage: compile <directory>" >&2
        return 1
    fi

    local name="$1"
    cd "$proj_dir/external/verinc-compiler"
    if [[ ! -x "$(pwd)/verinc" ]]; then
        echo "verinc compiler not found or not executable at $(pwd)/verinc" >&2
        cd - >/dev/null
        return 1
    fi

    local dir="$proj_dir/src/$name"
    local inc_file="$dir/$name.inc"
    local tla_file="$dir/$name.tla"
    local old_tla_file="$dir/$name.old"

    ./verinc $inc_file -o $dir || { echo "Failed to compile $name" >&2; cd - >/dev/null; return 1; }
    python3 $proj_dir/scripts/post_process.py $tla_file || { echo "Post-processing failed for $name" >&2; cd - >/dev/null; return 1; }
    java -cp lib/tla2tools.jar pcal.trans $tla_file > /dev/null 2>&1 || { echo "Pcal translation failed for $name" >&2; cd - >/dev/null; return 1; }
    rm -f $old_tla_file
    cd - > /dev/null
}

function compile_all() {
    echo "This function is not Bash-compatible and can only be executed in Zsh."

    local dir
    for dir in "$proj_dir"/src/term*(N) "$proj_dir"/src/trans*(N); do
        echo "Compiling $dir ..."
        compile "$(basename "$dir")" || return 1
    done
}
