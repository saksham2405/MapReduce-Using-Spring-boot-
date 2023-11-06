package  com.saksham.mapreduce.types;


import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.util.ArrayList;

@Data
@Accessors(chain = true)
public class KeyValuePair<KEY, VALUE> implements Serializable {
    private  KEY key;
    private  VALUE value;
}
