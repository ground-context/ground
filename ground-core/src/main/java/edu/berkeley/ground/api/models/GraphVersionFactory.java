package edu.berkeley.ground.api.models;

import edu.berkeley.ground.exceptions.GroundException;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public abstract class GraphVersionFactory {
    public abstract GraphVersion create(Optional<Map<String, Tag>> tags,
                                        Optional<String> structureVersionId,
                                        Optional<String> reference,
                                        Optional<Map<String, String>> parameters,
                                        String graphId,
                                        List<String> edgeVersionIds,
                                        List<String> parentIds) throws GroundException;

    public abstract GraphVersion retrieveFromDatabase(String id) throws GroundException;

    protected static GraphVersion construct(String id,
                                            Optional<Map<String, Tag>> tags,
                                            Optional<String> structureVersionId,
                                            Optional<String> reference,
                                            Optional<Map<String, String>> parameters,
                                            String graphId,
                                            List<String> edgeVersionIds) {

        return new GraphVersion(id, tags, structureVersionId, reference, parameters, graphId, edgeVersionIds);
    }
}
