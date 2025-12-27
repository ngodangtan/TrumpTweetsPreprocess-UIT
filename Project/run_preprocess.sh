#!/usr/bin/env bash
set -euo pipefail

MAIN_CLASS="TrumpTweetsPreprocess"
JAR_NAME="trump_preprocess.jar"
HADOOP_BIN="${HADOOP_BIN:-hadoop}"
JAVA_RELEASE="${JAVA_RELEASE:-11}"

# ✅ Với file CSV của bạn, default index trong Java đã đúng rồi.
# MR_PROPS vẫn giữ để bạn override nếu sau này đổi dataset.
MR_PROPS=(
  "-Dtweet.idx.date=${TWEET_IDX_DATE:-7}"
  "-Dtweet.idx.text=${TWEET_IDX_TEXT:-1}"
  "-Dtweet.idx.likes=${TWEET_IDX_LIKES:-5}"
  "-Dtweet.idx.retweets=${TWEET_IDX_RETWEETS:-6}"
  "-Dtweet.idx.is_retweet=${TWEET_IDX_IS_RETWEET:-2}"
)

die() { echo "❌ $*" >&2; exit 1; }
need_cmd() { command -v "$1" >/dev/null 2>&1 || die "Thiếu lệnh: $1"; }

usage() {
  cat <<EOF
Usage:
  ./run_preprocess.sh local <input_csv> <output_dir>
  ./run_preprocess.sh hdfs  <input_csv_local> <hdfs_input_path> <hdfs_output_path>

Examples:
  ./run_preprocess.sh local trump_tweets_raw.csv out_clean_local
  ./run_preprocess.sh hdfs trump_tweets_raw.csv /data/trump/trump_tweets_raw.csv /out/trump_clean

Override schema indexes (nếu CSV khác):
  TWEET_IDX_TEXT=1 TWEET_IDX_DATE=7 TWEET_IDX_LIKES=5 TWEET_IDX_RETWEETS=6 TWEET_IDX_IS_RETWEET=2 \\
  ./run_preprocess.sh local input.csv outdir
EOF
}

ensure_java11() {
  # Nếu bạn đã set JAVA_HOME thì giữ nguyên
  if [[ -n "${JAVA_HOME:-}" ]]; then return; fi

  # macOS: cố gắng lấy Java 11 nếu có
  if command -v /usr/libexec/java_home >/dev/null 2>&1; then
    local j11
    j11=$(/usr/libexec/java_home -v 11 2>/dev/null || true)
    if [[ -n "$j11" ]]; then
      export JAVA_HOME="$j11"
      export PATH="$JAVA_HOME/bin:$PATH"
    fi
  fi
}

build_jar() {
  need_cmd jar
  need_cmd javac

  echo "==> Build jar (target Java ${JAVA_RELEASE})..."
  rm -rf build
  mkdir -p build/classes

  local CP
  CP="$($HADOOP_BIN classpath --glob 2>/dev/null || true)"
  [[ -n "$CP" ]] || die "Không lấy được Hadoop classpath. Hãy đảm bảo 'hadoop' chạy được."

  javac -encoding UTF-8 \
    --release "${JAVA_RELEASE}" \
    -cp "$CP" \
    -d build/classes \
    "${MAIN_CLASS}.java"

  jar cf "build/${JAR_NAME}" -C build/classes .
  echo "✅ Built: build/${JAR_NAME}"
}

run_local() {
  local input="$1"
  local outdir="$2"

  [[ -f "$input" ]] || die "Không thấy file input: $input"
  rm -rf "$outdir" || true

  echo "==> Run MapReduce (LOCAL mode)..."
  $HADOOP_BIN jar "build/${JAR_NAME}" "$MAIN_CLASS" \
    "${MR_PROPS[@]}" \
    "$input" "$outdir"

  echo "==> Output preview:"
  if [[ -f "$outdir/part-r-00000" ]]; then
    head -n 5 "$outdir/part-r-00000"
  else
    echo "(Không thấy part-r-00000, kiểm tra output dir: $outdir)"
  fi
}

run_hdfs() {
  local input_local="$1"
  local hdfs_in="$2"
  local hdfs_out="$3"

  [[ -f "$input_local" ]] || die "Không thấy file input: $input_local"

  echo "==> Put input to HDFS..."
  $HADOOP_BIN fs -mkdir -p "$(dirname "$hdfs_in")" >/dev/null 2>&1 || true
  $HADOOP_BIN fs -put -f "$input_local" "$hdfs_in"

  echo "==> Remove old output (if exists)..."
  $HADOOP_BIN fs -rm -r -f "$hdfs_out" >/dev/null 2>&1 || true

  echo "==> Run MapReduce (HDFS mode)..."
  $HADOOP_BIN jar "build/${JAR_NAME}" "$MAIN_CLASS" \
    "${MR_PROPS[@]}" \
    "$hdfs_in" "$hdfs_out"

  echo "==> Output preview (HDFS):"
  $HADOOP_BIN fs -cat "$hdfs_out/part-r-00000" | head -n 5 || true
}

if [[ $# -lt 1 ]]; then usage; exit 1; fi

MODE="$1"; shift
ensure_java11

case "$MODE" in
  local)
    [[ $# -eq 2 ]] || { usage; exit 1; }
    build_jar
    run_local "$1" "$2"
    ;;
  hdfs)
    [[ $# -eq 3 ]] || { usage; exit 1; }
    build_jar
    run_hdfs "$1" "$2" "$3"
    ;;
  *)
    usage
    exit 1
    ;;
esac
