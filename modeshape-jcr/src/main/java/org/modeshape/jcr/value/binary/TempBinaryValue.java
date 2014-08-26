/*
 * ModeShape (http://www.modeshape.org)
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * See the AUTHORS.txt file in the distribution for a full listing of 
 * individual contributors.
 *
 * ModeShape is free software. Unless otherwise indicated, all code in ModeShape
 * is licensed to you under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 * 
 * ModeShape is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.modeshape.jcr.value.binary;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import javax.jcr.RepositoryException;
import org.modeshape.jcr.value.BinaryKey;

/**
 * A {@link BinaryValue} implementation that gets the content from the temporary File.
 * Is used as temporary storage of content stream for documents created in connector
 */
public class TempBinaryValue extends AbstractBinary {

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
            mimeType = "unknown";
        }
        return mimeType;
    }

    @Override
    public String getMimeType(String name) throws IOException, RepositoryException {
        if (mimeType == null) {
            mimeType = "unknown";
        }
        return mimeType;
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
    public void dispose() {
        if (tmpFile != null) {
            tmpFile.delete();
        }
    }

    @Override
    public long getSize() {
        return size;
    }

}
