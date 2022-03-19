import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.*;

public class ParallelOut extends BaseRichBolt {

    private FileWriter top100;
    private BufferedWriter top100Buffer;
    private int freq;

    SimpleDateFormat date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long startTime = System.currentTimeMillis();
    HashMap<String, Integer> hmap = new HashMap<String, Integer>();

    public void prepare(Map config, TopologyContext context, OutputCollector collector) {

        try {
            top100 = new FileWriter("/s/chopin/a/grad/mohitkum/twitter/parallel.txt", true);
            top100Buffer = new BufferedWriter(top100);

        } catch (Exception e) {
            System.out.println("Failed to write to the file 1 ");
            e.printStackTrace();
        }
        startTime = System.currentTimeMillis();

    }

    public void execute(Tuple tuple) {

        String list = tuple.getStringByField("list");
        freq = tuple.getIntegerByField("freq");
        displayOutput(list, freq);

    }





    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    public void cleanup() {
        try {
            top100Buffer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void displayOutput(String list, int freq) {
        long currentTime = System.currentTimeMillis();
        if (currentTime >= startTime + 10000 && !hmap.isEmpty()) {
            LinkedHashMap<String, Integer> sortedhm = sortHm(hmap);
            Collection<String> str;
            if (sortedhm.size() > 100) {
                str = Collections.list(Collections.enumeration(sortedhm.keySet())).subList(0, 100);
            }
            else {
                str = sortedhm.keySet();
            }
            Date newdate = new Date(startTime);
            try {
                top100Buffer.write("[" + date.format(newdate) + "]" + str.toString() + "\n");
                top100Buffer.flush();
            } catch (Exception e) {
                e.printStackTrace();
            }
            hmap.clear();
            startTime = currentTime;
        } else {
            if (!hmap.containsKey(list))
                hmap.put(list, freq);
        }
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