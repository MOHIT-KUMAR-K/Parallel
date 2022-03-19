import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ParallelAlgo extends BaseRichBolt {

    private OutputCollector coll;
    private int no_of_elements = 0;
    private Map<String, HashTagClass> wh = new ConcurrentHashMap<String, HashTagClass>();
    private double eps;
    private double threshold;
    private int bucket_size;

    private int current_Bucket = 1;
    private static long startTime;

    public ParallelAlgo(double e, double thres) {
        eps = e;
        threshold = thres;
        bucket_size = (int) Math.ceil(1 / eps);

    }

    public void prepare(Map config, TopologyContext context, OutputCollector coll) {

        this.coll = coll;
        startTime = System.currentTimeMillis();
    }

    public void execute(Tuple tuple) {

        String word;
        word = tuple.getStringByField("hashTag");
        coll.ack(tuple);
        lossyCounting(word);
    }

    public void lossyCounting(String word) {
        if (no_of_elements < bucket_size) {
            if (!wh.containsKey(word)) {
                HashTagClass hc = new HashTagClass();
                hc.delta = current_Bucket - 1;
                hc.freq = 1;
                hc.element = word;
                wh.put(word, hc);
            } else {
                HashTagClass hc = wh.get(word);
                hc.freq += 1;
                wh.put(word, hc);
            }
            no_of_elements += 1;
        }

        long currentTime = System.currentTimeMillis();
        if (currentTime >= startTime + 10000) {
            if (!wh.isEmpty()) {
                HashMap<String, Integer> emit_map = new HashMap<String, Integer>();
                for (String lossy_tags : wh.keySet()) {
                    boolean bool = check(lossy_tags);
                    if (bool) {
                        HashTagClass hc = wh.get(lossy_tags);
                        emit_map.put(lossy_tags, hc.freq);
                    }
                }
                if (!emit_map.isEmpty()) {

                    LinkedHashMap<String, Integer> sortedhm = sortHm(emit_map);
                    Collection<String> str;
                    if (sortedhm.size() > 100) {
                        str = Collections.list(Collections.enumeration(sortedhm.keySet())).subList(0, 100);
                    }
                    else {
                        str = sortedhm.keySet();
                    }
                    LinkedHashMap<String, Integer> finalEmit = new LinkedHashMap<String, Integer>();
                    for (String s : str) {
                        finalEmit.put("<" + s + ":" + wh.get(s).freq + ">", wh.get(s).freq);
                    }
                    for (String s : finalEmit.keySet()) {
                        coll.emit(new Values(s, finalEmit.get(s)));
                    }

                }
            }
            startTime = currentTime;

        }
        if (bucket_size == no_of_elements) {
            Delete();
            no_of_elements = 0;
            current_Bucket += 1;
        }
    }



    public boolean check(String lossyWord) {
        if (threshold == -1.0) {
            return true;
        } else {
            HashTagClass hc = wh.get(lossyWord);
            double val = (threshold - eps) * no_of_elements;
            if (hc.freq >= val)
                return true;
            else
                return false;
        }
    }

    public void Delete() {
        for (String word : wh.keySet()) {
            HashTagClass hc = wh.get(word);
            double sum = hc.freq + hc.delta;
            if (sum <= current_Bucket) {
                wh.remove(word);
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("list", "freq"));
    }

    public LinkedHashMap<String, Integer> sortHm(HashMap<String, Integer> inputmap) {
        List<String> listKeys = new ArrayList<String>(inputmap.keySet());
        List<Integer> listValues = new ArrayList<Integer>(inputmap.values());
        Collections.sort(listValues, Collections.reverseOrder());
        Collections.sort(listKeys, Collections.reverseOrder());

        LinkedHashMap<String, Integer> sortedhashMap = new LinkedHashMap<String, Integer>();

        Iterator<Integer> valueIterator = listValues.iterator();
        while (valueIterator.hasNext()) {
            Integer value = valueIterator.next();
            Iterator<String> keyIterator = listKeys.iterator();

            while (keyIterator.hasNext()) {
                String key = keyIterator.next();
                Integer val1 = inputmap.get(key);
                Integer val2 = value;

                if (val1.equals(val2)) {
                    keyIterator.remove();
                    sortedhashMap.put(key, value);
                    break;
                }
            }
        }
        return sortedhashMap;
    }

}