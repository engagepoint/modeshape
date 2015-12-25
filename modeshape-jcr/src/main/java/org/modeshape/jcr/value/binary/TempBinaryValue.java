/*
 * ModeShape (http://www.modeshape.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.modeshape.jcr.value.binary;

import org.modeshape.jcr.value.BinaryKey;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import javax.jcr.RepositoryException;

/**
 * A {@link AbstractBinary} implementation that gets the content from the temporary File.
 * Is used as temporary storage of content stream for documents created in connector
 */
public class TempBinaryValue extends AbstractBinary {

    public static final String UNKNOWN_MIME_TYPE = "unknown";

    private transient String mimeType;
    private final transient long size;
    private final File tmpFile;
    
    public TempBinaryValue(String mimeType, long size, BinaryKey key) throws IOException {
        super(key);
        this.tmpFile = new File(key.toString());
        this.mimeType = mimeType;
        this.size = size;        
    }
    
    public TempBinaryValue(String mimeType, long size, File tmpFile, BinaryKey key) throws IOException {
        super(key);
        this.tmpFile = tmpFile;
        this.mimeType = mimeType;
        this.size = size;        
    }

    @Override
    public String getMimeType() throws IOException, RepositoryException {
        if (mimeType == null) {
            mimeType = UNKNOWN_MIME_TYPE;
        }
        return mimeType;
    }

    @Override
    public String getMimeType(String name) throws IOException, RepositoryException {
        return getMimeType();
    }

    @Override
    protected InputStream internalStream() throws RepositoryException {
        try {
            return new FileInputStream(tmpFile);
        } catch (FileNotFoundException e) {
            throw new RepositoryException("already disposed", e);
        }
    }

    @Override
    public long getSize() {
        return size;
    }

}
