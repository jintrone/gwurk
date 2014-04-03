package edu.msu.mi.gwurk

class ShiroRole {
    String name

    static hasMany = [ users: ShiroUser, permissions: String ]
    static belongsTo = ShiroUser

    static constraints = {
        name(nullable: false, blank: false, unique: true)
    }
}
