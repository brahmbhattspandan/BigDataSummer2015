import java.io.IOException;
 
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
 
// Inherit from EvalFunc<Tuple> to implement a EvalFunc that returns a Tuple
public class LogParser extends EvalFunc<Tuple> {
 
    // The main method in question. Gets run for every 'thing' that gets sent to
    // this UDF
    public Tuple exec(Tuple input) throws IOException {
        if (null == input || input.size() != 1) {
            return null;
        }
        
        String line = (String) input.get(0);
        try {
            // In Soviet Russia, factory builds you!
            TupleFactory tf = TupleFactory.getInstance();
            Tuple t = tf.newTuple();
 
            t.append(getHttpMethod());
            t.append(getIP());
            t.append(getDate());
 
            // The tuple we are returning now has 3 elements, all strings.
            // In order, they are the HTTP method, the IP address, and the date.
 
            return t;
        } catch (Exception e) {
            // Any problems? Just return null and this one doesn't get
            // 'generated' by pig
            return null;
        }
    }
 
    public Schema outputSchema(Schema input) {
        try {
            Schema s = new Schema();
 
            s.add(new Schema.FieldSchema("action", DataType.CHARARRAY));
            s.add(new Schema.FieldSchema("ip", DataType.CHARARRAY));
            s.add(new Schema.FieldSchema("date", DataType.CHARARRAY));
 
            return s;
        } catch (Exception e) {
            // Any problems? Just return null...there probably won't be any
            // problems though.
            return null;
        }
    }
 
    public String getHttpMethod() {
        return "";
    }
 
    public String getIP() {
        return "";
    }
 
    public String getDate() {
        return "";
    }
}