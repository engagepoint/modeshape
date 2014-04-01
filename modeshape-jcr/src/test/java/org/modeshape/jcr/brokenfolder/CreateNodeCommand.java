package org.modeshape.jcr.brokenfolder;

import org.modeshape.jcr.JcrSession;

import javax.jcr.Node;
import javax.jcr.RepositoryException;

/**
 * @author evgeniy.shevchenko
 * @version 1.0 3/24/14
 */

public class CreateNodeCommand implements NodeCommand {

    private final String parentFolderId;

    public CreateNodeCommand(final String parentFolderId) {
        this.parentFolderId = parentFolderId;
    }

    @Override
    public NodeTaskResult execute(
            final JcrSession session,
            final String identifier)
            throws RepositoryException {
        Node parentFolder = session.getNodeByIdentifier(parentFolderId);
        Node node = parentFolder.addNode(identifier, "acme:DeckClassType");
        session.save();
        NodeTaskResult result = new NodeTaskResult();
        result.setNodeId(node.getIdentifier());
        result.setNodeName(node.getName());
        return  result;
    }


}
