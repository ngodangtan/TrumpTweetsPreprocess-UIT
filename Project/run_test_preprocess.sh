#!/usr/bin/env bash
set -euo pipefail

RAW="${1:-}"
OUT_PATH="${2:-}"

die(){ echo "❌ $*" >&2; exit 1; }

[[ -n "$RAW" || -n "$OUT_PATH" ]] || die "Usage: ./run_test_preprocess.sh <raw_csv_or_-> <output_or_->"
# Cho phép truyền "-" để bỏ qua input hoặc output
if [[ "$RAW" != "-" ]]; then
  [[ -f "$RAW" ]] || die "Không thấy input CSV: $RAW"
fi

PART="$OUT_PATH"
if [[ "$OUT_PATH" != "-" ]]; then
  if [[ -d "$OUT_PATH" ]]; then
    [[ -f "$OUT_PATH/part-r-00000" ]] || die "Không thấy $OUT_PATH/part-r-00000"
    PART="$OUT_PATH/part-r-00000"
  fi
  [[ -f "$PART" ]] || die "Không thấy output: $PART"
fi

# =========================
# IN INPUT (nếu có)
# =========================
if [[ "$RAW" != "-" ]]; then
  LINES=$(wc -l < "$RAW" | tr -d ' ')
  echo "=== INPUT (RAW) ==="
  echo "File: $RAW"
  echo "Số dòng: $LINES"
  echo ""

  HAS_URL=$(grep -Eic 'https?://|www\.' "$RAW" || true)
  HAS_MENTION=$(grep -Eic '@[A-Za-z0-9_]+' "$RAW" || true)
  HAS_HASHTAG=$(grep -Eic '#[A-Za-z0-9_]+' "$RAW" || true)
  HAS_RT=$(grep -Eic '(^|,|")RT[[:space:]]' "$RAW" || true)

  NONASCII=$(python3 - "$RAW" <<'PY'
import sys
path=sys.argv[1]
cnt=0
with open(path,'rb') as f:
    for line in f:
        try:
            s=line.decode('utf-8')
        except:
            cnt+=1
            continue
        if any(ord(ch)>127 for ch in s):
            cnt+=1
print(cnt)
PY
)

  echo "Dòng có URL:            $HAS_URL"
  echo "Dòng có mention (@...): $HAS_MENTION"
  echo "Dòng có hashtag (#...): $HAS_HASHTAG"
  echo "Dòng có RT (retweet):   $HAS_RT"
  echo "Dòng có ký tự lạ/emoji: $NONASCII"
  echo ""
  echo "Kết luận INPUT: ❌ KHÔNG PASS"
  echo ""
fi

# =========================
# IN OUTPUT (nếu có)
# =========================
if [[ "$OUT_PATH" != "-" ]]; then
  LINES=$(wc -l < "$PART" | tr -d ' ')
  FIRST_LINE=$(head -n 1 "$PART" || true)

  DELIM="|"
  if ! echo "$FIRST_LINE" | grep -q "|"; then
    DELIM=","
  fi

  AWK_FS=","
  if [[ "$DELIM" == "|" ]]; then
    AWK_FS="\\|"
  else
    AWK_FS=","
  fi

  echo "=== OUTPUT (SAU TIỀN XỬ LÝ) ==="
  echo "File: $PART"
  echo "Số dòng: $LINES"
  echo "Delimiter: $DELIM"
  echo ""

  awk -v FS_RE="$AWK_FS" -v D="$DELIM" '
  BEGIN{
    FS=FS_RE;
    bad_nf=0; bad_date=0; bad_like=0; bad_rt=0;
    bad_empty=0; bad_url=0; bad_hash=0; bad_mention=0;
    bad_upper=0; bad_chars=0;
    manual_rt=0;
  }
  function is_leap(y){ return ( (y%400==0) || (y%4==0 && y%100!=0) ) }
  function valid_ymd(y,m,dd,   md){
    if(m<1 || m>12) return 0;
    md[1]=31; md[2]=(is_leap(y)?29:28); md[3]=31; md[4]=30; md[5]=31; md[6]=30;
    md[7]=31; md[8]=31; md[9]=30; md[10]=31; md[11]=30; md[12]=31;
    if(dd<1 || dd>md[m]) return 0;
    return 1;
  }
  function valid_date_any(d,   y,m,dd){
    if(d ~ /^[0-9]{4}-[0-9]{2}-[0-9]{2}$/){
      y=substr(d,1,4)+0; m=substr(d,6,2)+0; dd=substr(d,9,2)+0;
      return valid_ymd(y,m,dd);
    }
    if(d ~ /^[0-9]{2}\/[0-9]{2}\/[0-9]{4}$/){
      dd=substr(d,1,2)+0; m=substr(d,4,2)+0; y=substr(d,7,4)+0;
      return valid_ymd(y,m,dd);
    }
    return 0;
  }
  {
    if(NF < 4){ bad_nf++; next; }

    date=$1; likes=$2; rts=$3;
    text=$4; if(NF>4){ for(i=5;i<=NF;i++) text=text D $i; }

    if(!valid_date_any(date)) bad_date++;
    if(likes !~ /^[0-9]+$/) bad_like++;
    if(rts !~ /^[0-9]+$/) bad_rt++;

    if(text ~ /^[[:space:]]*$/) bad_empty++;
    if(text ~ /(https?:\/\/|www\.)/) bad_url++;
    if(text ~ /#/) bad_hash++;
    if(text ~ /@/) bad_mention++;
    if(text ~ /[A-Z]/) bad_upper++;
    if(text !~ /^[a-z ]+$/) bad_chars++;

    # thống kê
    if(text ~ /^rt( |$)/) manual_rt++;
  }
  END{
    print "Sai số cột (NF<4):      " bad_nf;
    print "Date sai/không hợp lệ:  " bad_date;
    print "Likes không hợp lệ:     " bad_like;
    print "Retweets không hợp lệ:  " bad_rt;
    print "clean_text rỗng:        " bad_empty;
    print "còn URL:                " bad_url;
    print "còn hashtag (#):        " bad_hash;
    print "còn mention (@):        " bad_mention;
    print "còn chữ hoa:            " bad_upper;
    print "còn ký tự lạ:           " bad_chars;
    print "manual rt (chỉ thống kê): " manual_rt;

    fail = bad_nf+bad_date+bad_like+bad_rt+bad_empty+bad_url+bad_hash+bad_mention+bad_upper+bad_chars;
    print "";
    if(fail==0) print "Kết luận OUTPUT: PASS";
    else print "Kết luận OUTPUT: FAIL";
  }' "$PART"
fi
