package net.andesconsulting.dbf;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manejador de archivos DBF.<br/> Posee métodos que son análogos a los
 * comandos de Foxpro.
 *
 * @author Diego Enrique Silva Límaco (dsilva@andesconsulting.net)
 *
 */
public class DbfFile {

    static final Logger logger = Logger.getLogger(DbfFile.class.getCanonicalName());
    private int type;
    private Date lastUpdate;
    private int count;
    private int dataOffset;
    private int recordWidth;
    private int tableFlag;
    private Field[] fields;
    private int pointer;
    private RandomAccessFile file;
    private boolean deleted;

    public DbfFile(String nombre) throws FileNotFoundException {
        this(new File(nombre));

    }

    public DbfFile(File file) throws FileNotFoundException {
        if (file.exists()) {
            this.file = new RandomAccessFile(file, "rwd");
        } else {
            throw new FileNotFoundException();
        }
    }

    public void open() throws IOException {
        byte[] data = new byte[1024 * 3];

        file.read(data, 0, data.length);
        logger.log(Level.INFO, "header:" + data);
        // type
        type = data[0];
        logger.log(Level.INFO, "type:" + type);

        // Last update (YYMMDD)
        Calendar cal = Calendar.getInstance();
        cal.set((data[1] < 1980 ? 2000 : 1980) + (int) data[1], (int) data[2],
                (int) data[3]);
        lastUpdate = cal.getTime();
        logger.log(Level.INFO, "last update:" + lastUpdate);

        // cantidad de registros

        count = array2integer(new byte[]{data[4], data[5], data[6], data[7]});

        logger.log(Level.INFO, "count:" + count);
        // inicio de datos
        dataOffset = array2integer(new byte[]{data[8], data[9]});
        logger.log(Level.INFO, "data offset:" + dataOffset);
        // ancho de cada registro
        recordWidth = array2integer(new byte[]{data[10], data[11]});
        logger.log(Level.INFO, "record width:" + recordWidth);
        // flag de la tabla
        tableFlag = data[28];
        logger.log(Level.INFO, "table flag:" + recordWidth);
        // info de campos

        List<Byte> $data = new ArrayList<Byte>();
        for (int i = 32; data[i] != 0x0d; i += 32) {

            byte[] bloque = Arrays.copyOfRange(data, i, i + 32);

            Collection<Byte> $bloque = new ArrayList<Byte>();
            for (byte $byte : bloque) {
                $bloque.add($byte);
            }
            $data.addAll($bloque);
        }
        loadFieldsStructure($data);

        logger.log(Level.INFO, "fields count:" + fields.length);
        pointer = 1;
    }

    public int skip() throws IOException {
        return skip(1);
    }

    private void loadFieldsStructure(List<Byte> $data) {

        logger.log(Level.INFO, "size fields structure:" + $data.size());
        List<Field> $fields = new ArrayList<Field>();
        for (int i = 0; i < $data.size(); i += 32) {
            byte[] $segment = extractArray($data, i, 32);
            $fields.add(Field.newInstance($segment));
        }
        fields = $fields.toArray(new Field[0]);

    }

    private static byte[] extractArray(List<Byte> data, int from, int count) {
        byte[] $ret = new byte[count];
        for (int i = 0; i < count; i++) {
            if (data.size() > (from + i)) {
                $ret[i] = data.get(from + i);
            }
        }
        return $ret;
    }

    private static byte[] extractArray(byte[] data, int from, int count) {
        byte[] $ret = new byte[count];
        for (int i = 0; i < count; i++) {
            $ret[i] = data[from + i];
        }
        return $ret;
    }

    /*
     * private static int parseBin(Byte[] bs) { byte[] bb = new byte[bs.length];
     * for (int i = 0; i < bs.length; i++) bb[i] = bs[i]; return parseBin(bb); }
     */
    private static int array2integer(byte[] bs) {
        int ret = 0;
        for (int i = 0; i < bs.length; i++) {
            ret |= ((bs[i] & 0x0ff) << (i * 8));
        }
        return ret;
    }

    public void close() throws IOException {
        logger.info("Closing");
        file.close();

    }

    public Date getLastUpdate() {
        return lastUpdate;

    }

    public int getType() {
        return type;
    }

    public String getTypeName() {
        switch (type) {
            case 0x02:
                return "FoxBase";
            case 0x03:
                return "FoxBASE+/Dbase III plus, no memo";
            case 0x30:
                return "Visual FoxPro";
            case 0x31:
                return "Visual FoxPro, autoincrement enabled";
            case 0x43:
                return "dBASE IV SQL table files, no memo";
            case 0x63:
                return "dBASE IV SQL system files, no memo";
            case 0x83:
                return "FoxBASE+/dBASE III PLUS, with memo";
            case 0x8B:
                return "dBASE IV with memo";
            case 0xCB:
                return "dBASE IV SQL table files, with memo";
            case 0xF5:
                return "FoxPro 2.x (or earlier) with memo";
            case 0xFB:
                return "FoxBASE";
        }
        return null;

    }

    public int getCount() {
        return count;
    }

    public int recCount() {
        return getCount();
    }

    public int getDataOffset() {
        return dataOffset;
    }

    public int getRecordWidth() {
        return recordWidth;
    }

    public int getTableFlag() {
        return tableFlag;
    }

    public boolean hasStructuralCDX() {
        return (tableFlag & 0x01) == 0x01;
    }

    public boolean hasMemoField() {
        return (tableFlag & 0x02) == 0x02;
    }

    public boolean isDatabase() {
        return (tableFlag & 0x04) == 0x04;
    }

    public boolean isDeleted() {
        return deleted;
    }

    public void setDeleted(boolean deleted) throws IOException {
        this.deleted = deleted;
        go(pointer);
    }

    public static class Field {

        String name = "";
        char type;
        int offset;
        short length;
        short decimalPlaces;
        short flag;
        int nextAutovalue;
        int stepAutovalue;

        static Field newInstance(byte[] data) {
            Field f = new Field();
            for (int i = 0; i < 10 && data[i] != 0; i++) {
                f.name += (char) data[i];
            }
            f.type = (char) data[11];
            logger.log(Level.FINE, "field name  :" + f.name);
            f.offset = array2integer(new byte[]{data[12], data[13], data[14],
                        data[15]});

            logger.log(Level.FINE, "field offset:" + f.offset);
            if (f.type == 'C') {
                ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[]{data[17], data[16]});
                f.length = byteBuffer.asShortBuffer().get(0);
            }
            else
                f.length = data[16];
            logger.log(Level.FINE, "field length:" + f.length);

            f.decimalPlaces = data[17];
            logger.log(Level.FINE, "field dec.  :" + f.decimalPlaces);
            f.flag = data[18];
            logger.log(Level.FINE, "field flag  :" + f.flag);
            f.nextAutovalue = array2integer(new byte[]{data[19], data[20],
                        data[21], data[22]});
            f.stepAutovalue = data[23];
            logger.log(Level.INFO, f.name + "\t" + f.type + "\t" + f.length);
            return f;
        }
        /*
         * public Field(String name, char type, int offset, short length, short
         * decimalPlaces, short flag) { this(); this.name = name; this.type =
         * type; this.offset = offset; this.length = length; this.decimalPlaces =
         * decimalPlaces; this.flag = flag; }
         *
         * public Field() { }
         */
        public short getDecimalPlaces() {
            return decimalPlaces;
        }

        public short getFlag() {
            return flag;
        }

        public short getLength() {
            return length;
        }

        public String getName() {
            return name;
        }

        public int getNextAutovalue() {
            return nextAutovalue;
        }

        public int getOffset() {
            return offset;
        }

        public int getStepAutovalue() {
            return stepAutovalue;
        }

        public char getType() {
            return type;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    /**
     * Cambia el apuntador al registro index. Si Index es menor a 1, el nuevo
     * valor serï¿½ 1. Si Index es mayor a la cantidad de registros, el puntero se
     * ubicarï¿½ en el EOF, es decir en un registro siguiente al ï¿½ltimo.
     *
     * @param index El nuevo registro a apuntar
     * @return La nueva ubicacion
     */

    /* Cuando se accede a un registro lógicamente borrado se avanza hasta
     * encontrar un registro no borrado. Si no hay registros no borrados se
     * levanta una excepción de IOException.
     */
    public int go(int index) throws IOException {
        pointer = ((index > 0) ? ((index > count) ? (count + 1) : index) : 1);
        if (isDeleted()) {
            while (isDeletedRecord() && (! eof())) {
                pointer ++;
            }
            if (eof())
                throw new IOException();
        }
        return pointer;
    }

    public boolean isDeletedRecord() throws IOException {
        file.seek(dataOffset + (pointer - 1) * recordWidth);
        byte[] data = new byte[recordWidth];
        file.read(data);
        return data[0] != ' ';
    }

    public int go(Position pos) throws IOException {
        switch (pos) {
            case TOP:
                return go(1);
            case BOTTOM:
                return go(count);
        }
        return pointer;

    }

    public int skip(int delta) throws IOException {
        pointer += delta;
        return go(pointer);
    }

    /**
     * Devuelve la actual posiciï¿½n del puntero de registros
     *
     * @return La posiciï¿½n del puntero
     */
    public int recNo() {
        return pointer;
    }

    /**
     * Devuelve si el puntero de registros se encuentra despuï¿½s del ï¿½ltimo
     * registro
     *
     * @return un valor lï¿½gico
     */
    public boolean eof() {
        return recNo() > count;
    }

    /**
     * Agrega un registro en blanco al DBF. Si no logra hacerlo, retornarï¿½ la
     * misma cantidad de registros antes de hacer el Append.
     *
     * @return La cantidad de registros despues del Append
     * @throws IOException
     */
    public int append() throws IOException {
        if (updateHeader(4, 4, count + 1)) {

            byte[] data = new byte[recordWidth];
            Arrays.fill(data, (byte) ' ');
            file.seek(dataOffset + (count) * recordWidth);
            file.write(data);
            pointer = ++count;

        }
        return count;
    }

    /**
     * Retorna los valores del registro actual. Devuelve un mapa con el nombre
     * del campo y el valor asociado al registro
     *
     * @return El mapa de valores.
     * @throws IOException
     *             si ocurre un error al leer los datos
     */
    public Map<String, String> scatter() throws IOException {
        Map<String, String> ret = new HashMap<String, String>();
        file.seek(dataOffset + (pointer - 1) * recordWidth);
        byte[] data = new byte[recordWidth];
        file.read(data);
        StringBuffer sb = new StringBuffer();
        String fieldValue;
        for (Field field : fields) {
            byte[] arr = extractArray(data, field.offset, field.length);
            if (field.getType() == 'I') {
                ByteBuffer byteBuffer = ByteBuffer.wrap(arr);
                byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
                fieldValue = new Integer(byteBuffer.asIntBuffer().get(0)).toString();
            }

            else     {
                for (byte a : arr) {
                    sb.append((char) (a & 0xff));
                }
                fieldValue = sb.toString().trim();
            }
            ret.put(field.getName(), fieldValue);
            sb.setLength(0);
            sb.trimToSize();
        }
        return ret;
    }

    /**
     * Guarda los datos de record en el registro actual.
     *
     * @param record
     *            Un mapa con los nombres de los campos como clave, y con el
     *            valor a almacenar
     * @throws IOException
     *             si ocurre un error al guardar los datos
     */
    public void gatter(Map<String, String> record) throws IOException {

        byte[] data = new byte[recordWidth];
        Arrays.fill(data, (byte) ' ');
        for (Field field : fields) {
            String value = record.get(field.getName());
            if (value != null) {
                value2array(value, data, field.offset, field.length);
            }
        }
        file.seek(dataOffset + (pointer - 1) * recordWidth);
        file.write(data);
    }

    private static void value2array(String value, byte[] data, int offset,
            short length) {

        for (int i = 0; i < length && i < value.length(); i++) {
            data[i + offset] = (byte) value.charAt(i);
        }

    }

    private boolean updateHeader(int offset, int len, int data)
            throws IOException {

        byte[] arr = integer2array(data, len);

        file.seek(offset);
        file.write(arr);

        return true;

    }

    static private byte[] integer2array(int data, int count) {
        byte[] ret = new byte[count];
        for (int i = 0; i < count; i++) {
            ret[i] = (byte) (data & 0x0ff);
            data = data >> 8;
        }
        return ret;
    }

    public static enum Position {

        TOP, BOTTOM
    }

    public Field[] getFields() {
        return fields;
    }
}
