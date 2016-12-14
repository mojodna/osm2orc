package net.mojodna.osm2orc.standalone.parser;

import net.mojodna.osm2orc.standalone.model.Changeset;

public interface ChangesetCallback {
    void call(Changeset changeset);
}
