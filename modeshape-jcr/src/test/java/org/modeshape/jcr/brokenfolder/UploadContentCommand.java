package org.modeshape.jcr.brokenfolder;

import org.modeshape.jcr.JcrSession;

import javax.jcr.Node;
import javax.jcr.RepositoryException;

/**
 * @author evgeniy.shevchenko
 * @version 1.0 3/24/14
 */

public class UploadContentCommand implements NodeCommand {



    @Override
    public NodeTaskResult execute(
            final JcrSession session,
            final String identifier)
            throws RepositoryException {

        Node file = session.getNodeByIdentifier(identifier);
        Node resource = file.addNode("jcr:content", "nt:resource");
        resource.setProperty(
                "jcr:data",
                session.getValueFactory()
                        .createBinary("Body".getBytes()));
        session.save();
        NodeTaskResult result = new NodeTaskResult();
        result.setNodeId(file.getIdentifier());
        result.setNodeName(file.getName());
        result.setNodeContentId(resource.getIdentifier());
        return  result;

    }
}

