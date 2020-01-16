package com.allstar.nifi.processor.keycloak;

public class UserRepresentation {
    protected String id;
    protected String origin;
    protected Long createdTimestamp;
    protected String username;
    protected Boolean enabled;
    protected Boolean totp;
    protected Boolean emailVerified;
    protected String firstName;
    protected String lastName;
    protected String email;
    protected String federationLink;
    protected String serviceAccountClientId; // For rep, it points to clientId (not DB ID)

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getCreatedTimestamp() {
        return createdTimestamp;
    }

    public void setCreatedTimestamp(Long createdTimestamp) {
        this.createdTimestamp = createdTimestamp;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public Boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(Boolean enabled) {
        this.enabled = enabled;
    }

    @Deprecated
    public Boolean isTotp() {
        return totp;
    }

    @Deprecated
    public void setTotp(Boolean totp) {
        this.totp = totp;
    }

    public Boolean isEmailVerified() {
        return emailVerified;
    }

    public void setEmailVerified(Boolean emailVerified) {
        this.emailVerified = emailVerified;
    }


    public String getFederationLink() {
        return federationLink;
    }

    public void setFederationLink(String federationLink) {
        this.federationLink = federationLink;
    }

    public String getServiceAccountClientId() {
        return serviceAccountClientId;
    }

    public void setServiceAccountClientId(String serviceAccountClientId) {
        this.serviceAccountClientId = serviceAccountClientId;
    }

    /**
     * Returns id of UserStorageProvider that loaded this user
     *
     * @return NULL if user stored locally
     */
    public String getOrigin() {
        return origin;
    }

    public void setOrigin(String origin) {
        this.origin = origin;
    }


}
