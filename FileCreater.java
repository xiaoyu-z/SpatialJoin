import org.apache.commons.csv.*;

import java.io.*;
import java.nio.file.*;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class FileCreater {
    public static String file_name1 = "P.csv";
    public static String file_name2 = "R.csv";

    public static void main(String[] args) throws IOException {

        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(file_name1));
            CSVPrinter csvWriter = new CSVPrinter(writer, CSVFormat.DEFAULT);
            for (int i = 1; i <= 8000; i++) {
                csvWriter.printRecord(generate_points(i));
            }
            csvWriter.flush();

            BufferedWriter trans_riter = new BufferedWriter(new FileWriter(file_name2));
            CSVPrinter transWriter = new CSVPrinter(trans_riter, CSVFormat.DEFAULT);
            for (int i = 1; i <= 4000; i++) {
                transWriter.printRecord(generate_rect(i));
            }
            transWriter.flush();
        } catch (Exception e) {
            System.out.print(e);
        }

    }

    public static List<String> generate_points(int i) {
        Random random = new Random();
        float range = 9999.0f;
        DecimalFormat point_format = new DecimalFormat("#.0");
        String point_x = point_format.format(random.nextFloat() * range + 1.0f);
        String point_y = point_format.format(random.nextFloat() * range + 1.0f);
        return Arrays.asList(point_x, point_y);

    }

    public static List<String> generate_rect(int j) {
        Random random = new Random();
        float range = 9999.0f;
        float h_range = 19.0f;
        float w_range = 4.0f;
        float x;
        float y;
        float h;
        float w;
        DecimalFormat format = new DecimalFormat("#.0");
        //guarantee that all the rectangles are within the space
        while (true) {
            x = random.nextFloat() * range + 1.0f;
            y = random.nextFloat() * range + 1.0f;
            h = random.nextFloat() * h_range + 1.0f;
            w = random.nextFloat() * w_range + 1.0f;
            if (x + w <= 10000 && y + h <= 10000) break;

        }

        String point_x = format.format(x);
        String point_y = format.format(y);
        String height = format.format(h);
        String width = format.format(w);


        return Arrays.asList("r" + String.valueOf(j), point_x, point_y, height, width);

    }

}


