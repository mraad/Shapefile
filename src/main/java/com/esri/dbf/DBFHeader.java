package com.esri.dbf;

import org.apache.commons.io.EndianUtils;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 */
public class DBFHeader implements Serializable
{

    private byte signature;              /* 0     */
    public byte year;                   /* 1     */
    public byte month;                  /* 2     */
    public byte day;                    /* 3     */
    public int numberOfRecords;         /* 4-7   */
    public short headerLength;          /* 8-9   */
    public short recordLength;          /* 10-11 */
    private short reserved1;             /* 12-13 */
    private byte incompleteTransaction;  /* 14    */
    private byte encryptionFlag;         /* 15    */
    private int freeRecordThread;        /* 16-19 */
    private int reserved2;               /* 20-23 */
    private int reserved3;               /* 24-27 */
    private byte mdxFlag;                /* 28    */
    private byte languageDriver;         /* 29    */
    private short reserved4;             /* 30-31 */
    public List<DBFField> fields;       /* each 32 bytes */
    public int numberOfFields;

    public static DBFHeader read(final DataInputStream dataInput) throws IOException
    {
        final DBFHeader header = new DBFHeader();

        header.signature = dataInput.readByte();                           /* 0     */
        header.year = dataInput.readByte();                                /* 1     */
        header.month = dataInput.readByte();                               /* 2     */
        header.day = dataInput.readByte();                                 /* 3     */
        header.numberOfRecords = EndianUtils.readSwappedInteger(dataInput); //DbfUtils.readLittleEndianInt(dataInput);  /* 4-7   */

        header.headerLength = EndianUtils.readSwappedShort(dataInput);//DbfUtils.readLittleEndianShort(dataInput);   /* 8-9   */
        header.recordLength = EndianUtils.readSwappedShort(dataInput);//DbfUtils.readLittleEndianShort(dataInput);   /* 10-11 */

        header.reserved1 = dataInput.readShort();//DbfUtils.readLittleEndianShort(dataInput);        /* 12-13 */
        header.incompleteTransaction = dataInput.readByte();               /* 14    */
        header.encryptionFlag = dataInput.readByte();                      /* 15    */
        header.freeRecordThread = dataInput.readInt();//DbfUtils.readLittleEndianInt(dataInput); /* 16-19 */
        header.reserved2 = dataInput.readInt();                              /* 20-23 */
        header.reserved3 = dataInput.readInt();                              /* 24-27 */
        header.mdxFlag = dataInput.readByte();                             /* 28    */
        header.languageDriver = dataInput.readByte();                      /* 29    */
        header.reserved4 = dataInput.readShort();//DbfUtils.readLittleEndianShort(dataInput);        /* 30-31 */

        header.fields = new ArrayList<DBFField>();
        DBFField field;
        while ((field = DBFField.read(dataInput)) != null)
        {
            header.fields.add(field);
        }
        header.numberOfFields = header.fields.size();
        return header;
    }

    public DBFField getField(final int i)
    {
        return fields.get(i);
    }
}
