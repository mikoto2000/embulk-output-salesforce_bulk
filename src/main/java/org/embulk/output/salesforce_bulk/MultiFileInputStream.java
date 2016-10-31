package org.embulk.output.salesforce_bulk;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * MultiFileInputStream.
 */
public class MultiFileInputStream extends InputStream {
    private int currentFileIndex;
    private List<String> filePaths;
    private File currentFile;
    private FileInputStream currentFileInputStream;

    private static final int READ_END = -1;

    /**
     * コンストラクタ。
     *
     * @param filePaths 読み込むファイルパスの配列
     * @throws IOException InputStream のオープンエラー
     */
    public MultiFileInputStream(String[] filePaths)
            throws IOException {
        this(Arrays.asList(filePaths));
    }

    /**
     * コンストラクタ。
     *
     * @param filePaths 読み込むファイルパスの配列
     * @throws IOException InputStream のオープンエラー
     */
    public MultiFileInputStream(List<String> filePaths)
            throws IOException {
        currentFileIndex = 0;

        this.filePaths = new ArrayList<>(filePaths);

        currentFile = new File(filePaths.get(currentFileIndex));
        currentFileInputStream = new FileInputStream(currentFile);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int readedByte = currentFileInputStream.read(b, off, len);

        // ファイルを読み切ったら current を close して
        // 次のファイルを開く。
        if (readedByte == READ_END) {
            // 次のファイルが開けたら、それに対して read を行う。
            if (openNextFile()) {
                return read(b, off, len);
            }
        }

        return readedByte;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int read() throws IOException {
        int readedByte = currentFileInputStream.read();

        // ファイルを読み切ったら current を close して
        // 次のファイルを開く。
        if (readedByte == READ_END) {
            // 次のファイルが開けたら、それに対して read を行う。
            if (openNextFile()) {
                return read();
            }
        }

        return readedByte;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        currentFileInputStream.close();
    }

    /**
     * 次のファイルの InputStream を open する。
     *
     * @return 次のファイルを開けた場合 true
     * @throws IOException InputStream のオープンエラー
     */
    private boolean openNextFile() throws IOException {
        currentFileInputStream.close();
        currentFileIndex++;

        // 次のファイルがあるか確認
        if (currentFileIndex < filePaths.size()) {
            currentFile = new File(filePaths.get(currentFileIndex));
            currentFileInputStream = new FileInputStream(currentFile);
            return true;
        } else {
            return false;
        }
    }
}

