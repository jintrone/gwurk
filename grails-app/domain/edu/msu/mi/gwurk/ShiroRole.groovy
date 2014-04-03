package edu.msu.mi.gwurk

class ShiroRole {
    String name


    static mapping = {
        autoImport false
    }
    static hasMany = [ users: ShiroUser, permissions: String ]
    static belongsTo = ShiroUser

    static constraints = {
        name(nullable: false, blank: false, unique: true)
    }
}
