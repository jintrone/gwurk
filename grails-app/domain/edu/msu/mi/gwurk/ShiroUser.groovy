package edu.msu.mi.gwurk

class ShiroUser {
    String username
    String passwordHash

    static mapping = {
        autoImport false
    }
    
    static hasMany = [ roles: ShiroRole, permissions: String ]

    static constraints = {
        username(nullable: false, blank: false, unique: true)
    }
}
