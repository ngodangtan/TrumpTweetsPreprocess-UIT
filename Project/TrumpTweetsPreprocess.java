import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

public class TrumpTweetsPreprocess extends Configured implements Tool {

  // =========================================================
  // (Kết quả dữ liệu sau tiền xử lý) - Counters
  // =========================================================
  public enum PREP_COUNTER {
    TOTAL_LINES,
    HEADER_LINES,
    PARSE_ERROR,
    RETWEET_DROPPED,
    EMPTY_TEXT_DROPPED,
    VALID_OUTPUT
  }

  // =========================================================
  // (Làm sạch + Chuẩn hóa) - Regex
  // =========================================================
  private static final Pattern URL = Pattern.compile("https?://\\S+|www\\.\\S+");
  private static final Pattern MENTION = Pattern.compile("@\\w+");
  private static final Pattern HASHTAG = Pattern.compile("#\\w+");
  private static final Pattern NON_LETTER = Pattern.compile("[^a-z\\s]");
  private static final Pattern MULTI_SPACE = Pattern.compile("\\s+");

  // =========================================================
  // (Chuẩn hóa) - Stopwords cơ bản
  // =========================================================
  private static final Set<String> STOPWORDS = new HashSet<>(Arrays.asList(
      "a","an","the","and","or","but","if","then","else","for","to","of","in","on","at","by",
      "is","am","are","was","were","be","been","being","it","its","this","that","these","those",
      "i","you","he","she","we","they","me","my","your","our","their","him","her","them",
      "as","with","from","about","into","over","under","again","very","more","most","so","too"
  ));

  // =========================================================
  // CSV parser tối giản (xử lý dấu phẩy trong dấu ngoặc kép)
  // =========================================================
  private static List<String> parseCsvLine(String line) {
    List<String> out = new ArrayList<>();
    if (line == null) return out;

    StringBuilder cur = new StringBuilder();
    boolean inQuotes = false;

    for (int i = 0; i < line.length(); i++) {
      char c = line.charAt(i);

      if (c == '"') {
        if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
          cur.append('"');
          i++;
        } else {
          inQuotes = !inQuotes;
        }
      } else if (c == ',' && !inQuotes) {
        out.add(cur.toString());
        cur.setLength(0);
      } else {
        cur.append(c);
      }
    }
    out.add(cur.toString());
    return out;
  }

  private static String safeTrim(String s) {
    return s == null ? "" : s.trim();
  }

  // =========================================================
  // (Xử lý dữ liệu thiếu) - parse số, lỗi -> 0
  // =========================================================
  private static long parseLongOrZero(String s) {
    try {
      s = safeTrim(s);
      if (s.isEmpty()) return 0L;
      return Long.parseLong(s);
    } catch (Exception e) {
      return 0L;
    }
  }

  // =========================================================
  // (Làm sạch) - parse boolean: true/1/yes/y + t ; false/0/no/n + f
  // CSV của bạn dùng t/f cho isRetweet
  // =========================================================
  private static boolean parseBooleanLoose(String s) {
    s = safeTrim(s).toLowerCase();
    return s.equals("true") || s.equals("1") || s.equals("yes") || s.equals("y") || s.equals("t");
  }

  // =========================================================
  // (Chuẩn hóa) - date về YYYY-MM-DD
  // CSV date của bạn thường là dạng "YYYY-MM-DD HH:mm:ss" hoặc "YYYY-MM-DD"
  // =========================================================
  private static String normalizeDate(String raw) {
    raw = safeTrim(raw);
    if (raw.length() >= 10) return raw.substring(0, 10);
    return raw;
  }

  // =========================================================
  // (Làm sạch + Chuẩn hóa) - clean text
  // =========================================================
  private static String cleanText(String text) {
    text = safeTrim(text).toLowerCase();
    if (text.isEmpty()) return "";

    // Làm sạch: URL, mention, hashtag
    text = URL.matcher(text).replaceAll(" ");
    text = MENTION.matcher(text).replaceAll(" ");
    text = HASHTAG.matcher(text).replaceAll(" ");

    // Làm sạch: ký tự đặc biệt, chỉ giữ chữ cái
    text = NON_LETTER.matcher(text).replaceAll(" ");

    // Chuẩn hóa: khoảng trắng
    text = MULTI_SPACE.matcher(text).replaceAll(" ").trim();
    if (text.isEmpty()) return "";

    // Chuẩn hóa: remove stopwords
    StringBuilder sb = new StringBuilder();
    for (String w : text.split(" ")) {
      if (w.isEmpty()) continue;
      if (STOPWORDS.contains(w)) continue;
      sb.append(w).append(' ');
    }
    return sb.toString().trim();
  }

  // =========================================================
  // Mapper: tiền xử lý
  // =========================================================
  public static class PreprocessMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    /**
     * ✅ DEFAULT INDEX đúng theo file trump_tweets_raw.csv của bạn:
     * header: id,text,isRetweet,isDeleted,device,favorites,retweets,date,isFlagged
     * index : 0  1    2         3        4      5         6        7    8
     */
    private int IDX_TEXT = 1;
    private int IDX_IS_RETWEET = 2;
    private int IDX_LIKES = 5;     // favorites
    private int IDX_RETWEETS = 6;
    private int IDX_DATE = 7;

    @Override
    protected void setup(Context context) {
      // Cho phép override index bằng -Dtweet.idx.xxx nếu cần
      Configuration conf = context.getConfiguration();
      IDX_DATE = conf.getInt("tweet.idx.date", IDX_DATE);
      IDX_TEXT = conf.getInt("tweet.idx.text", IDX_TEXT);
      IDX_LIKES = conf.getInt("tweet.idx.likes", IDX_LIKES);
      IDX_RETWEETS = conf.getInt("tweet.idx.retweets", IDX_RETWEETS);
      IDX_IS_RETWEET = conf.getInt("tweet.idx.is_retweet", IDX_IS_RETWEET);
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

      context.getCounter(PREP_COUNTER.TOTAL_LINES).increment(1);

      String line = value.toString();
      if (line == null || line.trim().isEmpty()) {
        context.getCounter(PREP_COUNTER.PARSE_ERROR).increment(1);
        return;
      }

      // Bỏ header (an toàn hơn: check bắt đầu bằng "id,text,")
      String lower = line.toLowerCase();
      if (lower.startsWith("id,text,isretweet")) {
        context.getCounter(PREP_COUNTER.HEADER_LINES).increment(1);
        return;
      }

      List<String> cols = parseCsvLine(line);

      int need = Math.max(
          Math.max(IDX_DATE, IDX_TEXT),
          Math.max(Math.max(IDX_LIKES, IDX_RETWEETS), IDX_IS_RETWEET)
      );

      if (cols.size() <= need) {
        context.getCounter(PREP_COUNTER.PARSE_ERROR).increment(1);
        return;
      }

      try {
        String textRaw = cols.get(IDX_TEXT);
        String isRtRaw = cols.get(IDX_IS_RETWEET);
        String likesRaw = cols.get(IDX_LIKES);
        String retweetsRaw = cols.get(IDX_RETWEETS);
        String dateRaw = cols.get(IDX_DATE);

        // (Làm sạch) loại retweet
        if (parseBooleanLoose(isRtRaw)) {
          context.getCounter(PREP_COUNTER.RETWEET_DROPPED).increment(1);
          return;
        }

        // (Xử lý thiếu) likes/retweets -> 0 nếu rỗng/lỗi
        long likes = parseLongOrZero(likesRaw);
        long retweets = parseLongOrZero(retweetsRaw);

        // (Chuẩn hóa) date về YYYY-MM-DD
        String date = normalizeDate(dateRaw);

        // (Làm sạch + Chuẩn hóa) clean text
        String clean = cleanText(textRaw);
        if (clean.isEmpty()) {
          context.getCounter(PREP_COUNTER.EMPTY_TEXT_DROPPED).increment(1);
          return;
        }

        // (Kết quả dữ liệu sau tiền xử lý) output cho Chương 4
        // format: date,likes,retweets,clean_text
        String out = date + "," + likes + "," + retweets + "," + clean;

        context.write(NullWritable.get(), new Text(out));
        context.getCounter(PREP_COUNTER.VALID_OUTPUT).increment(1);

      } catch (Exception e) {
        context.getCounter(PREP_COUNTER.PARSE_ERROR).increment(1);
      }
    }
  }

  // Reducer identity: chỉ ghi output
  public static class IdentityReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
    @Override
    public void reduce(NullWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      for (Text v : values) {
        context.write(NullWritable.get(), v);
      }
    }
  }

  // =========================================================
  // ToolRunner: để Hadoop parse -D... mà không lẫn vào args chương trình
  // =========================================================
  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: TrumpTweetsPreprocess <input> <output>");
      return 2;
    }

    Configuration conf = getConf();
    Job job = Job.getInstance(conf, "Trump Tweets Preprocess");
    job.setJarByClass(TrumpTweetsPreprocess.class);

    job.setMapperClass(PreprocessMapper.class);
    job.setReducerClass(IdentityReducer.class);

    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    boolean ok = job.waitForCompletion(true);

    // In counters để bạn copy vào mục 3.7
    if (ok) {
      System.out.println("===== PREPROCESS SUMMARY (Counters) =====");
      for (PREP_COUNTER c : PREP_COUNTER.values()) {
        long v = job.getCounters().findCounter(c).getValue();
        System.out.println(c.name() + ": " + v);
      }
      System.out.println("========================================");
      return 0;
    }
    return 1;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new TrumpTweetsPreprocess(), args);
    System.exit(exitCode);
  }
}
